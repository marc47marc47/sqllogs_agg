use datafusion::prelude::*;
use datafusion::error::Result;
use std::path::Path;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. 從命令列參數讀取檔案名稱
    let args: Vec<String> = env::args().collect();
    let input_file = if args.len() > 1 {
        &args[1]
    } else {
        "data/sql_logs.tsv" // 預設檔案路徑
    };
    println!("Load File: {:?}", input_file);

    // 2. 判斷檔案是否為 .tsv
    if Path::new(input_file)
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        != "tsv"
    {
        eprintln!("Error: The input file must have a .tsv extension.");
        return Ok(());
    }

    // 3. 建立 DataFusion 的執行環境
    let ctx = SessionContext::new();

    // 4. 定義 CSV 的讀取選項 (tab 分隔，含標題)
    let csv_read_options = CsvReadOptions::new()
        .has_header(true)
        .delimiter(b'\t')
        .file_extension("tsv");

    // 5. 註冊 CSV 文件
    ctx.register_csv("sql_logs", input_file, csv_read_options)
        .await?;

    // 6. 使用 SQL 方法執行查詢
    let df = ctx
        .sql(
            "
            SELECT 
                sql_type, 
                date_trunc('day', exec_time) AS exec_day, 
                COUNT(*) AS request_count
            FROM sql_logs
            GROUP BY sql_type, date_trunc('day', exec_time)
            ORDER BY exec_day, sql_type
            ",
        )
        .await?;

    // 7. 顯示結果
    df.show().await?;
    Ok(())
}

