from dataretrieval import waterdata

# Retrieve daily data for a specific station
# parameterCd='00060' is for Discharge (Cubic Feet per Second)
df, metadata = waterdata.get_daily(
    monitoring_location_id="11447650",
    parameter_code="00060",
    time="2026-01-01/2026-04-21"
)
print(df.head())