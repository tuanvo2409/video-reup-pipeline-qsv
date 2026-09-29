@echo off
chcp 65001 >nul
title [VIDEO PIP] - CHẠY REUP HÀNG LOẠT SIÊU TỐC (INTEL QSV + SAMPLED-FRAME DHASH HEURISTIC)
color 0A

echo ===============================================================================
echo   🚀 HỆ THỐNG XỬ LÝ VIDEO REUP THEO CẤU HÌNH (GIAI ĐOẠN 1)
echo   • Gia tốc phần cứng: Intel Quick Sync (hevc_qsv)
echo   • Điều chỉnh tốc độ theo cảnh: PySceneDetect 26 phân cảnh biến thiên
echo   • Biến đổi khung hình: Zero-Border Zoom + Mask Vô Hình 98%% + Lật Gương
echo   • Tín hiệu tham khảo: sampled-frame dHash heuristic (<50%% similarity band)
echo   • Thư mục xuất: REUP_OUTPUT_DIR đã cấu hình (mặc định: output/ trong repository)
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

pause
