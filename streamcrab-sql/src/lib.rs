//! SQL layer for StreamCrab (P11).
//!
//! Pipeline: SQL text → datafusion-sql parser/planner → `LogicalPlan` →
//! translation into a chain of DataFusion-backed StreamCrab operators
//! (`DfFilterOp` / `DfProjectOp` / `DfGroupedSumOp`) → bounded execution
//! over registered `RecordBatch` sources (in-memory, Paimon scans, ...).
//!
//! v1 subset: single-table `SELECT` with `WHERE`, expression projections,
//! and `GROUP BY` + a single `SUM(col)`. Tumbling-window aggregation and
//! joins are follow-ups.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, TableReference};
use datafusion_expr::planner::ContextProvider;
use datafusion_expr::{
    AggregateUDF, Expr, LogicalPlan, ScalarUDF, TableSource, TableType, WindowUDF,
};
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use streamcrab_core::operator_chain::{Operator, TimerDomain};
use streamcrab_core::time::EVENT_TIME_MAX;
use streamcrab_vectorized::bridge::{DfFilterOp, DfGroupedSumOp, DfProjectOp};

type BatchOp = Box<dyn Operator<RecordBatch, OUT = RecordBatch> + Send>;

struct SimpleTableSource {
    schema: SchemaRef,
}

impl TableSource for SimpleTableSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

/// Registers bounded batch sources and executes SQL over them.
pub struct SqlContext {
    tables: HashMap<String, (SchemaRef, Vec<RecordBatch>)>,
    options: ConfigOptions,
}

impl Default for SqlContext {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlContext {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            options: ConfigOptions::default(),
        }
    }

    /// Register a bounded table from batches (e.g. a `PaimonSource` scan).
    pub fn register_batches(
        &mut self,
        name: impl Into<String>,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let schema = batches
            .first()
            .map(|b| b.schema())
            .context("cannot register an empty table: schema unknown")?;
        self.tables.insert(name.into(), (schema, batches));
        Ok(())
    }

    /// Plan and execute one SQL statement, returning the result batches.
    pub fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, query).context("parse SQL")?;
        if statements.len() != 1 {
            bail!("expected exactly one SQL statement, got {}", statements.len());
        }
        let plan = SqlToRel::new(self)
            .sql_statement_to_plan(statements.pop().expect("checked len"))
            .context("plan SQL statement")?;

        let mut exec = Execution::default();
        self.translate(&plan, &mut exec)?;
        let mut current = exec.source.context("query has no table scan")?;
        for op in exec.ops.iter_mut() {
            let mut out = Vec::new();
            op.process_batch(&current, &mut out)?;
            // Bounded input is fully consumed: fire the end-of-stream timer
            // so buffering operators (aggregates) emit.
            op.on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut out)?;
            current = out;
        }
        Ok(current)
    }

    /// Walk the logical plan bottom-up, pushing operators; returns the arrow
    /// schema of each stage's actual output.
    fn translate(&self, plan: &LogicalPlan, exec: &mut Execution) -> Result<SchemaRef> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                let name = scan.table_name.table().to_string();
                let (schema, batches) = self
                    .tables
                    .get(&name)
                    .with_context(|| format!("unknown table {name}"))?;
                let (schema, batches) = match &scan.projection {
                    Some(indices) => {
                        let s = Arc::new(schema.project(indices).context("project scan schema")?);
                        let b = batches
                            .iter()
                            .map(|b| b.project(indices))
                            .collect::<std::result::Result<Vec<_>, _>>()
                            .context("project scan batches")?;
                        (s, b)
                    }
                    None => (Arc::clone(schema), batches.clone()),
                };
                exec.source = Some(batches);
                Ok(schema)
            }
            LogicalPlan::Filter(filter) => {
                let input_schema = self.translate(&filter.input, exec)?;
                let predicate = unqualify(filter.predicate.clone())?;
                exec.ops
                    .push(Box::new(DfFilterOp::try_new(predicate, &input_schema)?));
                Ok(input_schema)
            }
            LogicalPlan::Projection(projection) => {
                let input_schema = self.translate(&projection.input, exec)?;
                let names: Vec<String> = projection
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                let exprs = projection
                    .expr
                    .iter()
                    .zip(names.iter())
                    .map(|(e, n)| Ok((unqualify(e.clone())?, n.as_str())))
                    .collect::<Result<Vec<_>>>()?;
                exec.ops
                    .push(Box::new(DfProjectOp::try_new(exprs, &input_schema)?));
                Ok(Arc::new(projection.schema.as_arrow().clone()))
            }
            LogicalPlan::Aggregate(agg) => {
                let input_schema = self.translate(&agg.input, exec)?;

                let key_cols = agg
                    .group_expr
                    .iter()
                    .map(|e| match e {
                        Expr::Column(c) => input_schema
                            .index_of(c.name())
                            .with_context(|| format!("group key {} not found", c.name())),
                        other => bail!("v1 GROUP BY supports plain columns, got {other}"),
                    })
                    .collect::<Result<Vec<_>>>()?;
                if agg.aggr_expr.len() != 1 {
                    bail!(
                        "v1 supports exactly one aggregate, got {}",
                        agg.aggr_expr.len()
                    );
                }
                let value_col = match &agg.aggr_expr[0] {
                    Expr::AggregateFunction(f) if f.func.name() == "sum" => {
                        match f.params.args.as_slice() {
                            [Expr::Column(c)] => input_schema
                                .index_of(c.name())
                                .with_context(|| format!("SUM column {} not found", c.name()))?,
                            other => bail!("v1 SUM takes a single column, got {other:?}"),
                        }
                    }
                    other => bail!("v1 supports SUM(col) only, got {other}"),
                };
                let output_name = agg.schema.field(key_cols.len()).name().clone();

                // The op's real output: key fields + Float64 sum (DataFusion's
                // logical plan may type SUM(bigint) as Int64; we accumulate in
                // f64, so report the actual schema for downstream compilation).
                let mut fields: Vec<Field> = key_cols
                    .iter()
                    .map(|&i| input_schema.field(i).clone())
                    .collect();
                fields.push(Field::new(&output_name, DataType::Float64, true));

                exec.ops.push(Box::new(
                    DfGroupedSumOp::new(key_cols, value_col).with_output_name(output_name),
                ));
                Ok(Arc::new(Schema::new(fields)))
            }
            LogicalPlan::SubqueryAlias(alias) => self.translate(&alias.input, exec),
            other => bail!("unsupported in v1 SQL subset: {}", other.display()),
        }
    }
}

#[derive(Default)]
struct Execution {
    source: Option<Vec<RecordBatch>>,
    ops: Vec<BatchOp>,
}

/// Strip table qualifiers so expressions compile against the unqualified
/// arrow schemas our operators carry (v1 is single-table).
fn unqualify(expr: Expr) -> Result<Expr> {
    Ok(expr
        .transform(|e| {
            Ok(match e {
                Expr::Column(c) => {
                    Transformed::yes(Expr::Column(Column::new_unqualified(c.name)))
                }
                other => Transformed::no(other),
            })
        })
        .context("strip column qualifiers")?
        .data)
}

impl ContextProvider for SqlContext {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let table = name.table();
        match self.tables.get(table) {
            Some((schema, _)) => Ok(Arc::new(SimpleTableSource {
                schema: Arc::clone(schema),
            })),
            None => Err(datafusion_common::DataFusionError::Plan(format!(
                "table {table} not registered"
            ))),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        if name.eq_ignore_ascii_case("sum") {
            Some(sum_udaf())
        } else {
            None
        }
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        vec!["sum".to_string()]
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}
