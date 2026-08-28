# 🚀 High-Performance Video Reup Pipeline (Intel QSV + Meta vPDQ Engine)

Hệ thống tự động hóa xử lý và lách bản quyền video ngắn (9:16 Vertical) tối ưu riêng cho **phần cứng iGPU Laptop (Intel Quick Sync Video `hevc_qsv`)** kết hợp **Phân cảnh biến thiên (`PySceneDetect`)** và **Kiểm định thuật toán Meta (`vPDQ Scorer`)**.

---

## ✨ 21 Tính Năng Nổi Bật (Hoạt Động Tự Động 100%)

1. ⚡ **Intel Quick Sync Video (`hevc_qsv`):** Render $100\%$ bằng phần cứng iGPU Intel Tiger Lake, CPU mát $< 15\%$, tốc độ $2.5\times - 3.5\times$ thời gian thực.
2. ✂️ **PySceneDetect Native Shot Detector:** Quét tự động phát hiện 26 nhịp cắt cảnh trong 4 giây.
3. ⏱️ **Nonlinear Dynamic Speed Modulation:** Bẻ tốc độ phi tuyến tính ngẫu nhiên theo từng cảnh ($1.10\times - 1.20\times$) làm méo chuỗi thời gian của AI.
4. 📐 **Zero-Border Center Zoom:** Zoom nhẹ $1.02\times - 1.05\times$ căn tâm chính giữa, **tràn viền $100\%$, không viền đen**.
5. 🪞 **Horizontal Mirroring (Lật gương trục X):** Đảo ngược không gian tọa độ điểm ảnh bẻ gãy ma trận dHash.
6. 👻 **Steganographic Invisible Mask:** Phủ ngầm lớp ảnh mờ $98\%$ lên mọi frame phá vỡ ma trận tensor điểm ảnh.
7. 🎬 **Head & Tail Trim:** Tự động cắt $0.8\text{s} - 1.2\text{s}$ đầu và đuôi để xóa logo intro/outro và đổi mốc timestamp.
8. 🌫️ **Micro-Grain Injection:** Chèn lớp hạt nhiễu vi mô thời gian thực phá vỡ thuật toán nén DCT.
9. 🎨 **Color Jitter (Lệch màu Histogram):** Lệch nhẹ Contrast, Brightness, Saturation ngẫu nhiên.
10. 🔊 **Audio Resampling:** Chuẩn hóa tần số `44100Hz Stereo AAC 192kbps`.
11. 🛡️ **Wipe Metadata & EXIF:** Xóa sạch $100\%$ thông tin camera, thiết bị gốc (`-map_metadata -1`).
12. 🔒 **Mã băm MD5 Mới:** Tự động tính toán và đối soát chữ ký số file độc lập.
13. 🏆 **Meta vPDQ & pHash Scorer:** Tự động chấm điểm đối soát sau khi render (**Đo đạt `45.88%` [PASSED]** - Vượt chuẩn kiểm duyệt $< 50\%$ của Meta/TikTok).
14. 💾 **Smart Processed Cache:** Ghi sổ nhật ký `processed_history.json`, nhận diện video cũ trong `0.1s` chống render trùng lặp.
15. 🔄 **Stale Cache Auto-Recovery:** Tự động cứu file dở dang về `input/` và dọn rác `tmp_` nếu máy tính bị tắt đột ngột.
16. 🌉 **Cầu nối tự động DUBVI (DUBVI Bridge):** Tự động chuyển tiếp video sạch sang thư mục của DUBVI để sẵn sàng dịch và lồng tiếng.
17. 🎭 **Mascot 3D Alpha Overlay:** Hỗ trợ chèn linh vật hoạt hình trong suốt góc dưới.
18. 🎬 **Ghép Hook/Outro ngẫu nhiên:** Hỗ trợ nối video mồi đầu/đuôi từ `assets/`.
19. 🎵 **BGM Layering:** Hỗ trợ trộn nhạc nền ngầm $8\%$ từ `assets/bgm/`.
20. 🏎️ **Single-Pass Filtergraph:** Gom toàn bộ 26 phân cảnh và bộ lọc vào **1 câu lệnh duy nhất trong RAM**.
21. 🤖 **Daemon Watcher 24/7:** Chế độ chạy ngầm tự động, hễ thả video vào là tự bốc render.

---

## 🚀 Hướng Dẫn Sử Dụng 1-Click

Chỉ cần **Nhấp đúp chuột** vào các file `.bat`:

* 🟢 **`Chay_Reup_Sieu_Toc.bat`**: Xử lý toàn bộ video trong `input/` $\rightarrow$ Tự động mở thư mục `video reup raw` khi xong.
* 🔵 **`Chay_Reup_Va_Chuyen_DUBVI.bat`**: Xử lý video và tự động chuyển sang thư mục của **DUBVI** để làm Giai đoạn 2 (Dịch thuật + Lồng tiếng CapCut).
* 🟡 **`Chay_Ngam_Tu_Dong_24_7.bat`**: Chế độ tự động chạy ngầm 24/7.

---

## 💻 Dòng Lệnh CLI Nâng Cao

```powershell
# 1. Chạy xử lý hàng loạt:
python watcher.py --batch --hflip

# 2. Ép render lại video kể cả khi đã có trong cache:
python watcher.py --batch --force --hflip

# 3. Tự động chuyển tiếp sang DUBVI:
python watcher.py --batch --hflip --to-dubvi

# 4. Tự chấm điểm vPDQ giữa 2 video bất kỳ:
python watcher.py --score "video_goc.mp4" "video_render.mp4"

# 5. Xem thống kê cache hoặc xóa cache:
python watcher.py --cache-stats
python watcher.py --clear-cache
```

---

## 📊 Kết Quả Đo Kiểm Thực Tế (Benchmark Metric):

```text
[Meta vPDQ Assessment]
• Original File: 1.mp4 (75.76s @ 2156x3836)
• Processed Master: processed_1.mp4 (64.03s @ 1080x1920 9:16)
• Visual Similarity: 45.88% (Threshold: < 50.0%)
• Status: [PASSED]
• Verdict: Non-duplicate / Unique fingerprint (Safe to post)
• Render Time: 88s (Intel QSV Hardware) | CPU Load: < 15%
```
