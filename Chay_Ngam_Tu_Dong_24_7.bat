@echo off
chcp 65001 >nul
title [VIDEO PIP] - CHẾ ĐỘ CHẠY NGẦM TỰ ĐỘNG 24/7 (DAEMON WATCHER)
color 0E

echo ===============================================================================
echo   🤖 CHẾ ĐỘ CHẠY NGẦM TỰ ĐỘNG 24/7 (DAEMON WATCHER)
echo   • Cứ mỗi khi bạn tải video về và thả vào thư mục 'input/'
echo   • Hệ thống sẽ tự động bốc và render ngay lập tức!
echo   • Nhấn Ctrl+C để dừng chương trình.
echo ===============================================================================
echo.

python watcher.py --watch --hflip --to-dubvi

pause
