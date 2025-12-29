# s-rim-analysis 폴더에서 실행
$structure = @(
    'pyproject.toml',
    '.env',
    'src/app/config.py',
    'src/app/db.py',
    'src/app/models.py',
    'src/app/etl/run_etl.py',
    'src/app/etl/sources_krx.py',
    'src/app/etl/sources_dart.py',
    'src/app/etl/compute.py',
    'src/app/etl/load.py',
    'src/app/api/main.py',
    'src/app/api/routes.py'
)

# 폴더들 먼저 생성
'src/app/etl', 'src/app/api' | ForEach-Object { mkdir $_ -Force }

# 파일들 생성
$structure | ForEach-Object { New-Item $_ -ItemType File -Force }
Write-Host "✅ Making S-rim-app structure completed!" -ForegroundColor Green
