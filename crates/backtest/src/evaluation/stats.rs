use super::{BootstrapConfig, ConfidenceInterval, MetricValue};

pub(crate) fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

pub(crate) fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let midpoint = sorted.len() / 2;
    Some(if sorted.len().is_multiple_of(2) {
        (sorted[midpoint - 1] + sorted[midpoint]) / 2.0
    } else {
        sorted[midpoint]
    })
}

pub(crate) fn sample_standard_deviation(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }

    let average = mean(values)?;
    let variance = values
        .iter()
        .map(|value| (value - average).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    Some(variance.sqrt())
}

/// Computes a two-sided Wilson score interval for a binomial proportion.
///
/// `confidence_level` must be strictly between zero and one. A zero-trial input
/// is reported as insufficient data rather than represented as `0 / 0`.
pub fn wilson_interval(
    successes: usize,
    trials: usize,
    confidence_level: f64,
) -> MetricValue<ConfidenceInterval> {
    if successes > trials {
        return MetricValue::invalid_input("successes cannot exceed trials");
    }
    if !confidence_level.is_finite() || !(0.0..1.0).contains(&confidence_level) {
        return MetricValue::invalid_input("confidence_level must be finite and between 0 and 1");
    }
    if trials == 0 {
        return MetricValue::insufficient_data("at least one trial is required");
    }

    let n = trials as f64;
    let estimate = successes as f64 / n;
    let z = inverse_standard_normal(0.5 + confidence_level / 2.0);
    let z_squared = z * z;
    let denominator = 1.0 + z_squared / n;
    let center = (estimate + z_squared / (2.0 * n)) / denominator;
    let margin =
        z * ((estimate * (1.0 - estimate) / n) + z_squared / (4.0 * n * n)).sqrt() / denominator;

    MetricValue::available(ConfidenceInterval {
        estimate,
        lower: (center - margin).max(0.0),
        upper: (center + margin).min(1.0),
        confidence_level,
    })
}

/// Computes a deterministic percentile-bootstrap confidence interval for a mean.
///
/// Resampling uses an internal SplitMix64 generator, making the output stable for
/// the same values, ordering, configuration, and crate version without adding a
/// random-number dependency.
pub fn bootstrap_mean_confidence(
    values: &[f64],
    config: BootstrapConfig,
) -> MetricValue<ConfidenceInterval> {
    if values.iter().any(|value| !value.is_finite()) {
        return MetricValue::invalid_input("bootstrap values must all be finite");
    }
    if !config.confidence_level.is_finite() || !(0.0..1.0).contains(&config.confidence_level) {
        return MetricValue::invalid_input("confidence_level must be finite and between 0 and 1");
    }
    if config.samples == 0 {
        return MetricValue::invalid_input("bootstrap samples must be greater than zero");
    }
    if values.len() < config.minimum_sample_size {
        return MetricValue::insufficient_data(format!(
            "at least {} observations are required for bootstrap confidence",
            config.minimum_sample_size
        ));
    }
    if values.is_empty() {
        return MetricValue::insufficient_data("at least one observation is required");
    }

    let estimate = mean(values).expect("non-empty values checked above");
    if !estimate.is_finite() {
        return MetricValue::invalid_input("bootstrap mean exceeds the finite f64 range");
    }

    let mut rng = SplitMix64::new(config.seed);
    let mut bootstrap_means = Vec::with_capacity(config.samples);
    for _ in 0..config.samples {
        let mut total = 0.0;
        for _ in values {
            total += values[rng.index(values.len())];
        }
        let bootstrap_mean = total / values.len() as f64;
        if !bootstrap_mean.is_finite() {
            return MetricValue::invalid_input(
                "a bootstrap sample mean exceeds the finite f64 range",
            );
        }
        bootstrap_means.push(bootstrap_mean);
    }
    bootstrap_means.sort_by(f64::total_cmp);

    let alpha = 1.0 - config.confidence_level;
    let lower = quantile_sorted(&bootstrap_means, alpha / 2.0);
    let upper = quantile_sorted(&bootstrap_means, 1.0 - alpha / 2.0);

    MetricValue::available(ConfidenceInterval {
        estimate,
        lower,
        upper,
        confidence_level: config.confidence_level,
    })
}

pub(crate) fn quantile_sorted(sorted: &[f64], probability: f64) -> f64 {
    debug_assert!(!sorted.is_empty());
    let index = probability * (sorted.len() - 1) as f64;
    let lower = index.floor() as usize;
    let upper = index.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let weight = index - lower as f64;
        sorted[lower] * (1.0 - weight) + sorted[upper] * weight
    }
}

// Peter J. Acklam's inverse-normal approximation. Accuracy is more than
// sufficient for confidence bounds and avoids a statistics dependency.
fn inverse_standard_normal(probability: f64) -> f64 {
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    const LOW: f64 = 0.024_25;
    const HIGH: f64 = 1.0 - LOW;

    if probability < LOW {
        let q = (-2.0 * probability.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if probability <= HIGH {
        let q = probability - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - probability).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    }
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn index(&mut self, upper_bound: usize) -> usize {
        ((self.next_u64() as u128 * upper_bound as u128) >> 64) as usize
    }
}
