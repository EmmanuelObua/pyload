from dotenv import dotenv_values

env = dotenv_values(".env")

outputFolder = 'C:\\Data\\output\\BB_General_ETL\\'
backupFolder = 'C:\\Data\\backup\\output_files\\BB_General_ETL\\'
outPrefix = 'GeneralEtlProcessing'

#log filepath
logPath = 'C:\\Data\\log\\BB_General_ETL\\'

#Target mysql credentials
tMysqlHost = env['MYSQL_HOST']
tMysqlPort = env['MYSQL_PORT']
tMysqlUser = env['MYSQL_USER_NAME']
tMysqlPassword = env['MYSQL_PASSWORD']

#email
emailConfig = {
    "Url": "http://utlhq407:9192/api/Email",
    "Dest": "ToCc",
    "From": " data.pipeline@utcl.co.ug",
    "To": "dl-bi_vas@utcl.co.ug",
    "Cc": "james.kamya@utcl.co.ug",
    "username": "VasApp",
    "password": "VasDev@1234"
}