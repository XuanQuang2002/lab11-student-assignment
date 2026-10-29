import structlog
import polars as pl
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

logger = structlog.get_logger()

BRONZE_DIR = "/opt/airflow/data/bronze"
SILVER_DIR = "/opt/airflow/data/silver"
DLQ_DIR = "/opt/airflow/data/dlq"


def _write_parquet_atomic(df, destination):
    """Publish a complete Parquet file without exposing an in-progress write."""
    destination = Path(destination)
    with NamedTemporaryFile(
        dir=destination.parent, prefix=f".{destination.name}.", suffix=".tmp",
        delete=False,
    ) as temporary:
        temporary_path = Path(temporary.name)
    try:
        df.write_parquet(temporary_path)
        os.replace(temporary_path, destination)
    finally:
        temporary_path.unlink(missing_ok=True)

def clean_to_silver(**kwargs):
    """
    Clean data from Bronze layer (Parquet) and load into Silver layer (Parquet).
    Implement Data Contracts and DLQ for invalid data.
    """
    run_id = kwargs.get('dag_run', None)
    log = logger.bind(run_id=run_id.run_id if run_id else "manual")
    
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(DLQ_DIR, exist_ok=True)
    
    try:
        # TODO: Đọc các file parquet từ BRONZE_DIR vào các DataFrame của polars
        # Gợi ý: pl.read_parquet(f"{BRONZE_DIR}/<table_name>.parquet")
        
        orders_df = pl.read_parquet(f"{BRONZE_DIR}/orders_dataset.parquet").unique(subset=["order_id"])
        customers_df = pl.read_parquet(f"{BRONZE_DIR}/customers_dataset.parquet")
        order_items_df = pl.read_parquet(f"{BRONZE_DIR}/order_items_dataset.parquet")
        products_df = pl.read_parquet(f"{BRONZE_DIR}/products_dataset.parquet")
        sellers_df = pl.read_parquet(f"{BRONZE_DIR}/sellers_dataset.parquet")
        geolocation_df = pl.read_parquet(f"{BRONZE_DIR}/geolocation_dataset.parquet")
        payments_df = pl.read_parquet(f"{BRONZE_DIR}/order_payments_dataset.parquet")
        reviews_df = pl.read_parquet(f"{BRONZE_DIR}/order_reviews_dataset.parquet")
        translation_df = pl.read_parquet(f"{BRONZE_DIR}/product_category_name_translation.parquet")

        
        # 1. Clean Customers
        log.info("Cleaning Customers...")
        customers_df = customers_df.unique(subset=["customer_id"])
        customers_df = customers_df.with_columns(pl.col("customer_city").str.to_lowercase())
        customers_df = customers_df.with_columns(pl.col("customer_state").str.to_uppercase())
        
        geolocation_df = geolocation_df.group_by("geolocation_zip_code_prefix").agg(
            pl.col("geolocation_lat").mean(),
            pl.col("geolocation_lng").mean(),
        )
        customers_df = customers_df.join(
            geolocation_df,
            left_on="customer_zip_code_prefix",
            right_on="geolocation_zip_code_prefix",
            how="left",
        )
        _write_parquet_atomic(customers_df, f"{SILVER_DIR}/dim_customers.parquet")
        
        
        # 2. Clean Sellers
        log.info("Cleaning Sellers...")
        sellers_df = sellers_df.drop_nulls(subset=["seller_id"]).unique(subset=["seller_id"])
        # TODO: Join với geolocation_df để lấy lat/lng
        sellers_df = sellers_df.join(
            geolocation_df,
            left_on="seller_zip_code_prefix",
            right_on="geolocation_zip_code_prefix",
            how="left",
        )
        # TODO: Lưu kết quả thành dim_sellers.parquet vào SILVER_DIR
        _write_parquet_atomic(sellers_df, f"{SILVER_DIR}/dim_sellers.parquet")
        
        # 3. Clean Products
        log.info("Cleaning Products...")
        # TODO: Loại bỏ các dòng trùng lặp ở product_id và fill null bằng strategy forward
        products_df = products_df.unique(subset=["product_id"])
        # TODO: Đổi tên các cột bị sai chính tả (vd: product_name_lenght -> product_name_length)
        products_df = products_df.rename({
            "product_name_lenght": "product_name_length",
            "product_description_lenght": "product_description_length",
        })
        # TODO: Join với translation_df để lấy tên danh mục tiếng anh
        products_df = products_df.join(
            translation_df,
            on="product_category_name",
            how="left",
        )
        _write_parquet_atomic(products_df, f"{SILVER_DIR}/dim_products.parquet")
        
        # 4. Transform Fact Order Items
        log.info("Transforming Fact Order Items...")
        orders_df = orders_df.with_columns([
             pl.col("order_purchase_timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
             pl.col("order_approved_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
             pl.col("order_delivered_carrier_date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
             pl.col("order_delivered_customer_date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
             pl.col("order_estimated_delivery_date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
             ])
        valid_orders = orders_df.filter(
            (pl.col("order_purchase_timestamp").is_null() | 
             pl.col("order_approved_at").is_null() | 
             (pl.col("order_purchase_timestamp") <= pl.col("order_approved_at"))) &
             (pl.col("order_approved_at").is_null() | 
              pl.col("order_delivered_carrier_date").is_null() | 
              (pl.col("order_approved_at") <= pl.col("order_delivered_carrier_date"))) &
              (pl.col("order_delivered_carrier_date").is_null() | 
               pl.col("order_delivered_customer_date").is_null() |
               (pl.col("order_delivered_carrier_date") <= pl.col("order_delivered_customer_date"))))
        
        fct_items_df = order_items_df.join(valid_orders, on="order_id", how="inner")
        valid_items_mask = (
            (fct_items_df["price"] > 0) &
            (fct_items_df["freight_value"] >= 0)
        ).fill_null(False)
        invalid_items_df = fct_items_df.filter(~valid_items_mask)
        fct_items_df = fct_items_df.filter(valid_items_mask)
        _write_parquet_atomic(fct_items_df, f"{SILVER_DIR}/fct_order_items.parquet")
        _write_parquet_atomic(invalid_items_df, f"{DLQ_DIR}/fct_order_items.parquet")
        
        
        # 5. Transform Fact Order Payments
        log.info("Transforming Fact Order Payments...")
        payments_df = payments_df.join(
            orders_df.select("order_id", "customer_id"),
            on="order_id",
            how="inner",
        )
        payments_df = payments_df.unique()
        _write_parquet_atomic(payments_df, f"{SILVER_DIR}/fct_order_payments.parquet")
        
        
        # 6. Transform Fact Order Reviews
        log.info("Transforming Fact Order Reviews...")
        reviews_df = reviews_df.join(
            orders_df,
            on="order_id",
            how="inner",
        )
        reviews_df = reviews_df.unique()
        _write_parquet_atomic(reviews_df, f"{SILVER_DIR}/fct_order_reviews.parquet")

        # Lưu thông tin chất lượng dữ liệu để task check_quality kiểm tra
        total_items = fct_items_df.height + invalid_items_df.height
        pass_quality = total_items > 0 and invalid_items_df.height * 10 < total_items
        kwargs['ti'].xcom_push(key="pass_quality", value=pass_quality)
        
        log.info("transform_silver_success")
                 
    except Exception as e:
        log.exception("transform_silver_failed")
        raise e

def check_quality(**kwargs):
    """
    Perform Data Quality checks on Silver data (inspired by Great Expectations).
    """
    passed = kwargs['ti'].xcom_pull(task_ids="transform_to_silver", key="pass_quality")
    log = logger.bind(task="check_quality")
    
    try:
        customer_quality = pl.scan_parquet(
            f"{SILVER_DIR}/dim_customers.parquet"
        ).select(
            pl.len().alias("rows"),
            pl.col("customer_id").null_count().alias("null_ids"),
            pl.col("customer_id").n_unique().alias("unique_ids"),
        ).collect().row(0, named=True)
        if customer_quality["rows"] == 0:
            log.error("DQ_FAIL: dim_customers has 0 rows.")
            passed = False
        if customer_quality["null_ids"] > 0:
            log.error("DQ_FAIL: dim_customers has null customer_id.")
            passed = False
        if customer_quality["unique_ids"] != customer_quality["rows"]:
            log.error("DQ_FAIL: dim_customers customer_id is not unique.")
            passed = False

        valid_statuses = [
            "delivered", "shipped", "invoiced", "processing", "approved",
            "canceled", "unavailable", "created",
        ]
        item_quality = pl.scan_parquet(
            f"{SILVER_DIR}/fct_order_items.parquet"
        ).select(
            pl.len().alias("rows"),
            (~pl.col("order_status").is_in(valid_statuses))
            .fill_null(True).sum().alias("invalid_statuses"),
        ).collect().row(0, named=True)
        if item_quality["rows"] == 0:
            log.error("DQ_FAIL: fct_order_items has 0 rows.")
            passed = False
        if item_quality["invalid_statuses"] > 0:
            log.error(
                f"DQ_FAIL: fct_order_items has {item_quality['invalid_statuses']} invalid order_status."
            )
            passed = False

        if passed:
            log.info("All Data Quality checks passed successfully!")
            return "load_to_gold"
        else:
            log.warning("Data Quality checks failed. Routing to alert.")
            return "alert_quality"
            
    except Exception as e:
        log.exception(f"Data Quality check encountered an error: {e}")
        return "alert_quality"

if __name__ == "__main__":
    # For local testing
    clean_to_silver(ti=type('obj', (object,), {'xcom_push': lambda **kw: None})())
