use crate::elastic::{ElasticConfig, ScalePolicy};
use std::time::Duration;

#[test]
fn test_elastic_config_defaults() {
    let config = ElasticConfig::default();
    assert_eq!(config.min_parallelism, 1);
    assert_eq!(config.max_parallelism, 64);
    assert_eq!(config.cooldown, Duration::from_secs(60));
    assert!(matches!(config.scale_policy, ScalePolicy::Manual));
}
