import subprocess

# List of python scripts to run
scripts = [
    'aapl_stock.py',
    'amzn_stock.py',
    'bac_stock.py',
    'dis_stock.py',
    'gld_etf.py',
    'googl_stock.py',
    'jpm_stock.py',
    'msft_stock.py',
    'spy_etf.py',
    'tsla_stock.py',
    'bito_etf.py'
]

for script in scripts:
    subprocess.run(["python3", script], cwd='/Users/JessFort/Library/Mobile Documents/com~apple~CloudDocs/My_Coding_folder/Tableau Project/data_extraction')
