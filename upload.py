import pandas as pd
from DeHaviland.API import PowerLedgerUploader

if __name__ == "__main__":
    # 1. CSV 文件路径（请根据实际情况修改）
    csv_file = 'merged_output.csv'

    try:
        # 读取 CSV 文件
        df = pd.read_csv(csv_file)
        print("原始 CSV 数据预览:")
        print(df.head())
    except Exception as e:
        print(f"读取 CSV 文件时出错: {e}")
        exit(1)

    # 2. 确保 timeStamp 符合 ISO8601 标准格式 (YYYY-MM-DDTHH:MM:SS+08:00)
    #    如果你的 CSV 已经是正确的 "+08:00" 格式，可省略以下转换逻辑
    try:
        # 尝试将 timeStamp 解析为 datetime 类型
        # 如果原本数据中没有冒号 (如 +0800)，解析后再转换可自动标准化
        df['timeStamp'] = pd.to_datetime(df['timeStamp'], errors='coerce')

        # 如果你确定所有时间都是东八区，可在这里进行本地化处理
        # 注：若数据中已包含时区信息，以下 localize 步骤可省略或调整
        # df['timeStamp'] = df['timeStamp'].dt.tz_localize('Asia/Shanghai')

        # 统一输出为 ISO8601 字符串，含 +08:00
        df['timeStamp'] = df['timeStamp'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')

        # 如果输出结果是 "+0800" 而非 "+08:00"，可通过正则插入冒号
        df['timeStamp'] = df['timeStamp'].str.replace(
            r'(\+|\-)(\d{2})(\d{2})',
            r'\1\2:\3',
            regex=True
        )
    except Exception as e:
        print(f"timeStamp 转换出错: {e}")
        exit(1)

    # 3. 按 meterUid 分组，并对每个分组内的缺失值进行填充（示例中填充为 0）
    grouped = df.groupby('meterUid').apply(lambda group: group.fillna(0)).reset_index(drop=True)

    print("分组并填充缺失值后的数据预览:")
    print(grouped.head())

    # 4. 将处理后的数据覆盖保存回 CSV 文件
    grouped.to_csv(csv_file, index=False)
    print(f"处理后的 CSV 数据已保存到 {csv_file}")

    # 5. 使用 PowerLedgerUploader 上传 CSV 文件
    uploader = PowerLedgerUploader(file_path=csv_file)
    try:
        uploader.authenticate()
        uploader.process_and_upload()
        print(f"文件 {csv_file} 上传成功。")
        # 如果上传成功后需要删除文件，请取消下面代码的注释
        # os.remove(csv_file)
    except Exception as e:
        print(f"上传过程中发生错误: {e}")
