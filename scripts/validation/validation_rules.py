def validate_sales_data(df):
    """Return dictionary of validation results"""
    report = {}
    if "customer_id" in df.columns:
        report["null_customer_id"] = df.filter(df.customer_id.isNull()).count()
    if "price" in df.columns:
        report["negative_price"] = df.filter(df.price < 0).count()
    report["duplicate_rows"] = df.count() - df.dropDuplicates().count()
    return report
