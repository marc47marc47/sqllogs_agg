use datafusion::prelude::*;
//use datafusion::datasource::file_format::csv::CsvReadOptions;
use std::sync::Arc;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 建立 DataFusion 的執行環境
    let mut ctx = SessionContext::new();

    // 定義 CSV 文件的讀取設定（tab 分隔）
    let csv_read_options = CsvReadOptions::new()
        .has_header(true)
        .delimiter(b'\t')  // 指定為 tab 分隔
        .file_extension("tsv"); // 如果文件不是 .csv，指定副檔名

    // 註冊 CSV 文件
    ctx.register_csv("sql_logs", "data/sql_logs.tsv", csv_read_options)
        .await?;

    // SQL 查詢：統計每小時的 sql_type, client_host 出現次數
    let df = ctx
        .sql(
            "
            SELECT 
                sql_type,
                date_trunc('day', exec_time) AS exec_hour, 
                COUNT(1) AS request_count
            FROM sql_logs
            GROUP BY sql_type, date_trunc('day', exec_time)
            ORDER BY exec_hour, sql_type
            ",
        )
        .await?;

    // 顯示查詢結果
    df.show().await?;
    Ok(())
}

