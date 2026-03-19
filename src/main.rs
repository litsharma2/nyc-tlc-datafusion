use anyhow::{anyhow, Context, Result};
use clap::Parser;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
use datafusion::prelude::*;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Directory containing parquet files, e.g. ./data/2025
    #[arg(long, default_value = "data/2025")]
    data_dir: PathBuf,

    /// Year used for filename pattern
    #[arg(long, default_value_t = 2025)]
    year: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let all_paths = build_monthly_paths(&args.data_dir, args.year)?;
    let paths = existing_paths_or_warn(&all_paths)?;

    let ctx = SessionContext::new();

    let df_all = ctx
        .read_parquet(paths.clone(), ParquetReadOptions::default())
        .await
        .context("failed to read parquet files")?;

    // Register for SQL
    ctx.register_table("trips", df_all.clone().into_view())
        .context("failed to register table trips")?;

    println!("Loaded {} parquet file(s) for year {}.", paths.len(), args.year);
    println!("Running aggregations using DataFusion DataFrame API and SQL...\n");

    // Aggregation 1
    run_agg1_dataframe(&df_all).await?;
    run_agg1_sql(&ctx).await?;

    // Aggregation 2
    run_agg2_dataframe(&df_all).await?;
    run_agg2_sql(&ctx).await?;

    println!("\nAll aggregations completed successfully.");
    Ok(())
}

fn build_monthly_paths(data_dir: &Path, year: i32) -> Result<Vec<String>> {
    let mut paths = Vec::with_capacity(12);
    for m in 1..=12 {
        let file = format!("yellow_tripdata_{year}-{m:02}.parquet");
        let full = data_dir.join(file);
        paths.push(full.to_string_lossy().to_string());
    }
    Ok(paths)
}

fn existing_paths_or_warn(all_paths: &[String]) -> Result<Vec<String>> {
    let mut existing = Vec::new();
    let mut missing = Vec::new();

    for p in all_paths {
        if Path::new(p).exists() {
            existing.push(p.clone());
        } else {
            missing.push(p.clone());
        }
    }

    if existing.is_empty() {
        return Err(anyhow!(
            "No parquet files found. Please download data into data/2025"
        ));
    }

    if !missing.is_empty() {
        println!(
            "WARNING: {} parquet file(s) missing. Some months may not be published yet.",
            missing.len()
        );
        for m in missing.iter().take(12) {
            println!("  missing: {m}");
        }
        println!();
    }

    Ok(existing)
}

async fn print_df(title: &str, df: DataFrame) -> Result<()> {
    let batches = df.collect().await.context("collect failed")?;
    let formatted = pretty_format_batches(&batches).context("pretty print failed")?;
    println!("{title}\n{formatted}\n");
    Ok(())
}

// ---------------- Aggregation 1 ----------------
async fn run_agg1_dataframe(df: &DataFrame) -> Result<()> {
    let month_expr = date_trunc(lit("month"), col("tpep_pickup_datetime")).alias("pickup_month");

    let out = df
        .clone()
        .aggregate(
            vec![month_expr],
            vec![
                count(lit(1)).alias("trip_count"),
                sum(col("total_amount")).alias("total_revenue"),
                avg(col("fare_amount")).alias("avg_fare"),
            ],
        )?
        .sort(vec![col("pickup_month").sort(true, true)])?;

    print_df("Aggregation 1 (DataFrame API): Trips and revenue by pickup month", out).await
}

async fn run_agg1_sql(ctx: &SessionContext) -> Result<()> {
    let sql = r#"
        SELECT
            date_trunc('month', tpep_pickup_datetime) AS pickup_month,
            COUNT(1) AS trip_count,
            SUM(total_amount) AS total_revenue,
            AVG(fare_amount) AS avg_fare
        FROM trips
        GROUP BY 1
        ORDER BY 1 ASC
    "#;

    let out = ctx.sql(sql).await?;
    print_df("Aggregation 1 (SQL): Trips and revenue by pickup month", out).await
}

// ---------------- Aggregation 2 (FIXED) ----------------
async fn run_agg2_dataframe(df: &DataFrame) -> Result<()> {
    // Step 1: aggregate sums separately (avoid sum()/sum() in one expression)
    let grouped = df
        .clone()
        .aggregate(
            vec![col("payment_type")],
            vec![
                count(lit(1)).alias("trip_count"),
                avg(col("tip_amount")).alias("avg_tip_amount"),
                sum(col("tip_amount")).alias("sum_tip_amount"),
                sum(col("total_amount")).alias("sum_total_amount"),
            ],
        )?;

    // Step 2: compute tip_rate in projection
    let out = grouped
        .select(vec![
            col("payment_type"),
            col("trip_count"),
            col("avg_tip_amount"),
            (col("sum_tip_amount") / col("sum_total_amount")).alias("tip_rate"),
        ])?
        .sort(vec![col("trip_count").sort(false, true)])?;

    print_df("Aggregation 2 (DataFrame API): Tip behavior by payment type", out).await
}

async fn run_agg2_sql(ctx: &SessionContext) -> Result<()> {
    let sql = r#"
        SELECT
            payment_type,
            COUNT(1) AS trip_count,
            AVG(tip_amount) AS avg_tip_amount,
            SUM(tip_amount) / NULLIF(SUM(total_amount), 0) AS tip_rate
        FROM trips
        GROUP BY 1
        ORDER BY trip_count DESC
    "#;

    let out = ctx.sql(sql).await?;
    print_df("Aggregation 2 (SQL): Tip behavior by payment type", out).await
}