"""Trust Guardian classifier for video content vetting."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

COMMERCIAL_KEYWORDS = [
    "好物", "推荐", "带货", "购物", "链接", "橱窗", "同款", "小黄车", "拍下", 
    "买这个", "买它", "买", "下单", "包邮", "优惠券", "优惠", "福利", "价格", "性价比", 
    "几块钱", "便宜", "只要几块", "到手价", "手慢无", "旗舰店", "正品", "百元", "几十块", "平价",
    "测评", "开箱", "拔草", "种草", "安利", "入手", "别买", "千万别买", "智商税", 
    "避坑指南", "避坑", "好用", "神器", "踩坑", "翻车", "不按说明书", "收纳神器",
    "点击左下角", "看橱窗", "去主页", "链接在评论区", "置顶链接", "商品",
    "耳塞", "垃圾桶", "收纳盒", "支架", "拖把", "吸尘器", "清洁剂", "床垫", "挂钩", "小家电",
    "折叠桌", "置物架", "收纳箱", "除螨", "香薰", "台灯", "插座", "收纳袋", "洗衣机", "冰箱"
]

PURE_TRUST_PATTERNS = [
    "室友", "舍友", "同居", "男生宿舍", "女生宿舍", "寝室", "大学生活", "疯狂大学生",
    "当代大学生", "当代年轻人的精神状态", "神仙室友", "奇葩室友", "室友迷惑行为",
    "突击", "爆改", "帮室友", "给室友", "退租", "房东", "押金", "地盘", "边界感",
    "争夺", "防室友", "邋遢", "吵架", "吐槽", "崩溃", "汗流浃背", "意想不到的结局",
    "记录真实生活", "独居日记", "恋爱日常", "合租"
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

    commercial_matches = [kw for kw in COMMERCIAL_KEYWORDS if kw in text_corpus]
    trust_matches = [pt for pt in PURE_TRUST_PATTERNS if pt in text_corpus]

    hard_reject = any(k in commercial_matches for k in ["小黄车", "橱窗", "带货", "拍下", "测评", "开箱", "种草", "好物", "几块钱", "神器"])
    if hard_reject or len(commercial_matches) >= 2:
        return {
            "is_trust": False, 
            "verdict": "REJECT_COMMERCIAL_KEYWORDS", 
            "matched_commercial": commercial_matches,
            "reason": f"Phát hiện từ khóa bán hàng/review: {', '.join(commercial_matches[:4])}"
        }

    if len(trust_matches) >= 1 and not hard_reject and len(commercial_matches) <= 1:
        return {
            "is_trust": True, 
            "verdict": "APPROVED_PURE_TRUST", 
            "matched_trust": trust_matches,
            "reason": f"Thuần Drama đời sống KTX / Xây Trust: {', '.join(trust_matches[:3])}"
        }

    if len(commercial_matches) == 0:
        return {"is_trust": True, "verdict": "APPROVED_ORGANIC_CONTENT", "reason": "Nội dung đời thường tự nhiên, không mùi bán hàng"}

    return {"is_trust": False, "verdict": "REJECT_SUSPICIOUS", "matched_commercial": commercial_matches, "reason": "Nghi vấn thương mại ngầm"}
