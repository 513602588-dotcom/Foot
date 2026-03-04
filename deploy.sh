#!/bin/bash
# 快速部署脚本 - Football Prediction System

set -e

echo "=========================================="
echo "⚽ Football Prophet Pro - Quick Deploy"
echo "=========================================="
echo ""

# 检查Python版本
echo "🔍 检查Python版本..."
python_version=$(python --version 2>&1 | awk '{print $2}')
echo "   Python $python_version"

if python -c 'import sys; sys.exit(0 if sys.version_info >= (3,9) else 1)' 2>/dev/null; then
    echo "   ✅ 版本符合要求 (>=3.9)"
else
    echo "   ❌ 需要Python 3.9或更高版本"
    exit 1
fi

echo ""

# 创建虚拟环境 (可选)
if [ "$1" == "venv" ]; then
    echo "📦 创建虚拟环境..."
    python -m venv venv
    source venv/bin/activate
    echo "   ✅ 虚拟环境已激活"
    echo ""
fi

# 安装依赖
echo "📦 安装依赖包..."
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
echo "   ✅ 依赖安装完成"
echo ""

# 创建目录结构
echo "📁 创建目录结构..."
mkdir -p logs data/cache models site/data
echo "   ✅ 目录结构已创建"
echo ""

# 配置文件
echo "⚙️  配置环境变量..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo "   ✅ .env 文件已创建"
    echo "   ⚠️  请编辑 .env 填入您的API密钥"
else
    echo "   ℹ️  .env 文件已存在"
fi
echo ""

# 数据库初始化
echo "🗄️  初始化数据库..."
python -c "from src.data.data_collector_enhanced import DataCollector; DataCollector()"
echo "   ✅ 数据库初始化完成"
echo ""

echo "=========================================="
echo "✅ 部署完成！"
echo "=========================================="
echo ""
echo "📚 下一步:"
echo "   1. 编辑 .env 文件填入API密钥"
echo "   2. 运行: python quick_start.py"
echo "   3. 或运行: python src/build_pipeline.py"
echo ""
echo "🌐 查看结果:"
echo "   python -m http.server 8000"
echo "   打开浏览器: http://localhost:8000/site/index_pro.html"
echo ""
