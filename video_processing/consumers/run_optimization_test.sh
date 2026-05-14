#!/bin/bash
# 快速启动优化测试脚本
# ========================

echo "=========================================="
echo "视频生成优化测试启动脚本"
echo "=========================================="
echo ""

# 检查Python环境
if ! command -v python &> /dev/null; then
    echo "❌ 错误: 未找到Python环境"
    exit 1
fi

echo "✅ Python环境检测通过"

# 检查必要的依赖
echo "🔍 检查依赖包..."
python -c "import playwright; import asyncio; import httpx" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "⚠️  检测到缺少依赖包，正在安装..."
    pip install playwright asyncio httpx
    if [ $? -ne 0 ]; then
        echo "❌ 依赖安装失败"
        exit 1
    fi
fi
echo "✅ 依赖包检测通过"

# 检查Playwright浏览器
echo "🔍 检查Playwright浏览器..."
playwright install chromium 2>/dev/null
if [ $? -ne 0 ]; then
    echo "⚠️  Playwright浏览器未安装，正在安装..."
    python -m playwright install chromium
fi
echo "✅ 浏览器检测通过"

# 设置环境变量
export FLOW_ENABLE_CONTEXT_STEALTH_SCRIPT="true"
export FLOW_BROWSER_UNIQUE_PER_TASK="1"
export FLOW_VIDEO_LOCALE="en-US"
export FLOW_VIDEO_TIMEZONE_ID="America/Los_Angeles"

echo "🌐 环境变量已设置:"
echo "   - FLOW_ENABLE_CONTEXT_STEALTH_SCRIPT=true"
echo "   - FLOW_BROWSER_UNIQUE_PER_TASK=1"
echo "   - FLOW_VIDEO_LOCALE=en-US"
echo "   - FLOW_VIDEO_TIMEZONE_ID=America/Los_Angeles"

# 创建日志目录
mkdir -p "$(dirname "$0")/../../log"
mkdir -p "$(dirname "$0")/../../video_processing/log"

echo ""
echo "=========================================="
echo "选择测试模式:"
echo "1. 快速测试 (2个帧模式任务)"
echo "2. 完整测试 (所有任务类型，每个10条)"
echo "3. Redis队列测试 (生产环境模拟)"
echo "4. 查看实时日志"
echo "=========================================="
echo ""
read -p "请选择 (1-4): " choice

case $choice in
    1)
        echo ""
        echo "🚀 启动快速测试..."
        python "$(dirname "$0")/test_anti_detection.py"
        ;;
    2)
        echo ""
        echo "🚀 启动完整测试..."
        python "$(dirname "$0")/run_40_task_audit.py" --frame-only
        ;;
    3)
        echo ""
        echo "🚀 启动Redis队列测试..."
        echo "📝 提示: 请手动启动消费者进程:"
        echo "   python -m video_processing.consumers.redis_task_consumer"
        echo ""
        python "$(dirname "$0")/test_anti_detection.py"  # 选择模式2
        ;;
    4)
        echo ""
        echo "📊 显示实时日志 (Ctrl+C退出):"
        echo "=========================================="
        tail -f "$(dirname "$0")/../../log/automation_video_consumer.log"
        ;;
    *)
        echo "❌ 无效选择"
        exit 1
        ;;
esac

exit_code=$?
echo ""
echo "=========================================="
if [ $exit_code -eq 0 ]; then
    echo "✅ 测试完成"
else
    echo "❌ 测试失败 (退出码: $exit_code)"
fi
echo "=========================================="

exit $exit_code