"""Trust Guardian classifier for video content vetting."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

# CÁC TỪ KHÓA BÁN HÀNG NẶNG (AFFILIATE / GIỎ HÀNG / SEEDING TRỰC TIẾP - LUÔN CẤM 100%)
HARD_COMMERCIAL_KEYWORDS = [
    "小黄车", "橱窗", "带货", "拍下", "下单", "链接", "点击左下角", "看橱窗", "去主页",
    "链接在评论区", "置顶链接", "包邮", "优惠券", "福利", "到手价", "手慢无", "旗舰店",
    "正品", "买它", "只要几块", "测评", "开箱", "种草", "好物", "收纳神器", "避坑指南",
    "避坑", "智商税", "商品", "买这个"
]

# CÁC TỪ KHÓA MÔ TẢ ĐỒ DÙNG / GIÁ CẢ THƯỜNG GẶP (CHO PHÉP TRONG NGỮ CẢNH DECOR / KTX)
SOFT_HOUSEHOLD_KEYWORDS = [
    "耳塞", "垃圾桶", "收纳盒", "支架", "拖把", "吸尘器", "清洁剂", "床垫", "挂钩", "小家电",
    "折叠桌", "置物架", "收纳箱", "除螨", "香薰", "台灯", "插座", "收纳袋", "洗衣机", "冰箱",
    "几块钱", "便宜", "价格", "性价比", "平价", "几十块", "百元", "买", "好用", "推荐", "同款"
]

COMMERCIAL_KEYWORDS = HARD_COMMERCIAL_KEYWORDS + SOFT_HOUSEHOLD_KEYWORDS

PURE_TRUST_PATTERNS = [
    # Drama KTX & Đời sống sinh viên / Bạn cùng phòng (Page 1)
    "室友", "舍友", "同居", "男生宿舍", "女生宿舍", "寝室", "大学生活", "疯狂大学生",
    "当代大学生", "当代年轻人的精神状态", "神仙室友", "奇葩室友", "室友迷惑行为",
    "突击", "爆改", "帮室友", "给室友", "退租", "房东", "押金", "地盘", "边界感",
    "争夺", "防室友", "邋遢", "吵架", "吐槽", "崩溃", "汗流浃背", "意想不到的结局",
    "记录真实生活", "独居日记", "恋爱日常", "合租",

    # Biến hình phòng trọ, Cải tạo, Dọn dẹp, Before-After, DIY (Page 2)
    "改造", "翻新", "变身", "逆袭", "前vs后", "前后对比", "装修", "简装", "刷漆", "贴墙纸",
    "房间", "卧室", "出租屋", "破烂屋", "猪窝", "狗窝", "老破小", "毛坯房", "单间",
    "打扫", "清洁", "大扫除", "沉浸式打扫", "断舍离", "扔垃圾", "洗刷", "去污", "大扫除日记",
    "整理", "收纳", "空间利用", "格局", "布局", "设计", "自己动手", "diy", "手工", "木工",
    "治愈", "解压", "独居", "女生独居", "男生独居", "北漂", "广漂", "沪漂", "租房日常", "省钱",
    "自律", "治愈系", "生活日记", "我的小家", "独居生活"
]


def classify_video_intent(
    desc: str = "",
    asr_transcript: str = "",
    extra_metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    text_corpus = f"{desc} {asr_transcript}".lower()
    meta = extra_metadata or {}
    
    if meta.get("is_ads") or meta.get("commercial_video"):
        return {"is_trust": False, "verdict": "REJECT_COMMERCIAL_ADS", "reason": "Cờ quảng cáo trả phí trên Douyin"}
    if bool(meta.get("commerce_info")) or bool(meta.get("anchor_info")):
        return {"is_trust": False, "verdict": "REJECT_COMMERCIAL_SHOP", "reason": "Có gắn link Giỏ hàng Douyin / Anchor"}

    hard_matches = [kw for kw in HARD_COMMERCIAL_KEYWORDS if kw in text_corpus]
    soft_matches = [kw for kw in SOFT_HOUSEHOLD_KEYWORDS if kw in text_corpus]
    trust_matches = [pt for pt in PURE_TRUST_PATTERNS if pt in text_corpus]

    # 1. Nếu dính từ khóa bán hàng nặng (gắn giỏ hàng, seeding, link mua, mã giảm giá) -> LOẠI THẲNG TAY
    if hard_matches:
        return {
            "is_trust": False,
            "verdict": "REJECT_HARD_COMMERCIAL",
            "matched_commercial": hard_matches,
            "reason": f"Phát hiện từ khóa bán hàng/seeding trực tiếp: {', '.join(hard_matches[:4])}"
        }

    # 2. Nếu có từ khóa Xây Trust (Drama KTX hoặc Cải tạo phòng / Dọn dẹp / DIY)
    # Cho phép miêu tả đồ dùng sinh hoạt / mẹo dọn dẹp thường ngày (soft words <= 4)
    if len(trust_matches) >= 1:
        if len(soft_matches) <= 4:
            return {
                "is_trust": True,
                "verdict": "APPROVED_PURE_TRUST",
                "matched_trust": trust_matches,
                "reason": f"Thuần đời thường / Cải tạo phòng / Xây Trust: {', '.join(trust_matches[:3])}"
            }
        else:
            return {
                "is_trust": False,
                "verdict": "REJECT_COMMERCIAL_KEYWORDS",
                "matched_commercial": soft_matches,
                "reason": f"Nhắc quá nhiều đồ gia dụng/review: {', '.join(soft_matches[:4])}"
            }

    # 3. Không có trust_matches nhưng cũng hoàn toàn không có từ soft nào
    if len(soft_matches) == 0:
        return {"is_trust": True, "verdict": "APPROVED_ORGANIC_CONTENT", "reason": "Nội dung đời thường tự nhiên, không mùi bán hàng"}

    # 4. Không có trust_matches mà lại có từ soft (>= 2 từ)
    if len(soft_matches) >= 2:
        return {"is_trust": False, "verdict": "REJECT_SUSPICIOUS", "matched_commercial": soft_matches, "reason": "Nghi vấn thương mại ngầm"}

    return {"is_trust": True, "verdict": "APPROVED_ORGANIC_CONTENT", "reason": "Nội dung đời thường tự nhiên"}
