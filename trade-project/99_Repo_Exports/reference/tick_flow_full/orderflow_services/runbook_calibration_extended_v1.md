# Calibration Extended (v1)

Key metrics:
- `conf_cal_extended_metric{arm="active",metric="mce_cal"}`
- `conf_cal_extended_metric{arm="active",metric="calibration_slope"}`
- `conf_cal_extended_metric{arm="active",metric="sharpness_mean"}`
- `conf_cal_extended_metric{arm="active",metric="prob_mass_near_half"}`

Interpretation:
- high `mce_cal` means one or more calibration bins are badly wrong even if global ECE is acceptable
- low `calibration_slope` means over/under-confidence or compressed probabilities
- low `sharpness_mean` / high `prob_mass_near_half` means probabilities are collapsing toward 0.5
