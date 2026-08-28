@echo off
chcp 65001 >nul
title [VIDEO PIP -> DUBVI] - CHẠY REUP & TỰ ĐỘNG CHUYỂN TIẾP SANG DUBVI
color 0B

echo ===============================================================================
echo   🚀 HỆ THỐNG LIÊN HOÀN 2 GIAI ĐOẠN: VIDEO PIP  ==^>  DUBVI AI
echo   • Giai đoạn 1: Bẻ gãy mã băm vPDQ + Render siêu tốc QSV
echo   • Chuyển tiếp: Tự động copy sang thư mục 'C:\Users\vmath\Videos\douyin'
echo   • Giai đoạn 2: Sẵn sàng để DUBVI Dịch thuật, Lồng tiếng CapCut & Dán Sub
echo ===============================================================================
echo.

echo [*] Đang xử lý video và tự động đồng bộ sang DUBVI...
echo.

python watcher.py --batch --hflip --to-dubvi

echo.
echo ===============================================================================
echo   🎉 HOÀN THÀNH VÀ ĐÃ CHUYỂN TIẾP THÀNH CÔNG SANG DUBVI!
echo   • Bạn có thể mở DUBVI Studio hoặc DUBVI Worker để dịch và lồng tiếng ngay.
echo ===============================================================================
echo.

explorer "C:\Users\vmath\Videos\douyin"
pause
