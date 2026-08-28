@echo off
chcp 65001 >nul
title [VIDEO PIP] - CHẠY REUP HÀNG LOẠT SIÊU TỐC (INTEL QSV + VPDQ PASSED)
color 0A

echo ===============================================================================
echo   🚀 HỆ THỐNG XỬ LÝ VIDEO REUP VƯỢT THUẬT TOÁN (GIAI ĐOẠN 1)
echo   • Gia tốc phần cứng: Intel Quick Sync (hevc_qsv)
echo   • Bẻ trục thời gian: PySceneDetect 26 phân cảnh biến thiên
echo   • Bẻ ma trận thị giác: Zero-Border Zoom + Mask Vô Hình 98%% + Lật Gương
echo   • Chuẩn kiểm định: Meta vPDQ PASSED (<50%% Match)
echo   • Thư mục xuất: C:\Users\vmath\Downloads\douyinnnnnnnnnnn\video reup raw
echo ===============================================================================
echo.

echo [*] Đang quét và xử lý toàn bộ video trong thư mục 'input/'...
echo.

python watcher.py --batch --hflip

echo.
echo ===============================================================================
echo   🎉 HOÀN THÀNH XỬ LÝ TOÀN BỘ VIDEO!
echo   • Video sạch mã băm đã được xuất ra thư mục 'video reup raw'
echo ===============================================================================
echo.

explorer "C:\Users\vmath\Downloads\douyinnnnnnnnnnn\video reup raw"
pause
