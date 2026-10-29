import structlog
import polars as pl
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.dialects.postgresql import insert

logger = structlog.get_logger()

SILVER_DIR = "/opt/airflow/data/silver"


def upsert_table(engine, df, table_name, schema, pk_columns):
    """UPSERT batches using SQLAlchemy directly, compatible with version 1.4."""
    with engine.begin() as conn:
        table = Table(table_name, MetaData(), schema=schema, autoload_with=conn)
        assert table is not None
        # Select the Gold contract explicitly; Silver may contain joined columns.
        columns = [column.name for column in table.c]
        records = df[columns].astype(object).where(df[columns].notna(), None)
        for start in range(0, len(records), 1000):
            statement = insert(table).values(
                records.iloc[start:start + 1000].to_dict(orient="records")
            )
            statement = statement.on_conflict_do_update(
                index_elements=pk_columns,
                set_={
                    column: statement.excluded[column]
                    for column in columns if column not in pk_columns
                },
            )
            conn.execute(statement)


def load_to_gold(**kwargs):
    """Load Silver dimensions and facts into the PostgreSQL Gold schema."""
    run_id = kwargs.get('dag_run')
    log = logger.bind(run_id=run_id.run_id if run_id else "manual")
    engine = create_engine(
        "postgresql+psycopg2://admin:password@datamart:5432/ecommerce"
    )
    tables = [
        ("dim_customers", ["customer_id"]),
        ("dim_products", ["product_id"]),
        ("dim_sellers", ["seller_id"]),
        ("fct_order_items", ["order_id", "order_item_id"]),
        ("fct_order_payments", ["order_id", "payment_sequential"]),
        ("fct_order_reviews", ["review_id", "order_id"]),
    ]
    timestamp_columns = [
        "shipping_limit_date", "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date", "review_creation_date",
        "review_answer_timestamp",
    ]
    try:
        for table_name, pk_columns in tables:
            log.info("Loading table", table=table_name)
            df = pl.read_parquet(f"{SILVER_DIR}/{table_name}.parquet").to_pandas()
            if table_name in ("dim_customers", "dim_sellers"):
                prefix = "customer" if table_name == "dim_customers" else "seller"
                df = df.rename(columns={
                    "geolocation_lat": f"{prefix}_lat",
                    "geolocation_lng": f"{prefix}_lng",
                })
            for column in timestamp_columns:
                if column in df.columns:
                    df[column] = df[column].astype('datetime64[ns]')
            upsert_table(engine, df, table_name, "gold", pk_columns)
        log.info("load_to_gold_success")
    except Exception:
        log.exception("load_to_gold_failed")
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    load_to_gold()
