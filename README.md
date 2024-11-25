統計欄位：

替換為 sql_type, exec_time 的小時截斷 (用 date_trunc('hour', exec_time) 實現) 和 COUNT。
使用 DataFrame 操作：

使用 DataFrame 的 select、aggregate 和 sort 方法進行操作。
移除重複匯入：

移除了 use datafusion::datasource::file_format::csv::CsvReadOptions;。
輸入檔案：

改為傳入檔案名稱 (input_file)。
檢查檔案副檔名：

判斷輸入檔案是否為 .tsv，如果不是則顯示錯誤訊息並退出。
輸入檔案 (sql_logs.tsv)
範例內容：

sql_type	exec_time	client_ip	client_host
SELECT	2024-11-26 14:15:00	192.168.1.1	host1
INSERT	2024-11-26 14:25:00	192.168.1.2	host2
SELECT	2024-11-26 15:05:00	192.168.1.3	host3
執行結果：

+-----------+---------------------+---------------+
| sql_type  | exec_hour           | request_count |
+-----------+---------------------+---------------+
| SELECT    | 2024-11-26 14:00:00 | 1             |
| SELECT    | 2024-11-26 15:00:00 | 1             |
| INSERT    | 2024-11-26 14:00:00 | 1             |
+-----------+---------------------+---------------+
