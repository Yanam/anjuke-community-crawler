# 安居客小区数据爬虫

## Requirements:
- python3
- python3-venv
- requests 2.18.4
- pyquery 1.2.4
- mysqlclient 1.3.12

## Installation

```bash
cd Crawler
make install
```

## Configuration
```bash
vim config.ini
```

## Execution

```bash
cd Crawler
make
```

## Files

    ├── LazyFW
    │   ├── __init__.py
    ├── Makefile
    ├── README.md
    ├── config.ini              # 配置文件
    ├── cookies.txt             # 访问一下安居客，获取Cookies
    ├── main.py                 # 主程序
    ├── requirements.txt
    └── ve
