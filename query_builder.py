USE_CASE_REQUIREMENTS = {
    "дизайн": {
        "min_ram": 8,
        "min_p_cores": 8,
        "gpu_type": ["design"],
        "min_ssd": 512,
    },
    "игры": {
        "min_ram": 16,
        "min_p_cores": 6,
        "gpu_type": ["gaming_discrete"],
        "min_refresh_rate": 120,
        "min_ssd": 1000,
        "is_gaming_flag": True
    },
    "программирование": {
        "min_ram": 16,
        "min_p_cores": 6,
        "min_ssd": 256
    },
    "фильмы": {
        "min_ram": 8,
        "min_ssd": 512,
    },
    "документы": {
        "min_ram": 8,
        "min_ssd": 256,
    },
    "учеба": {
        "min_ram": 8,
        "min_ssd": 256,
        "min_battery": 8,
    }
}


def build_sql_query(filters: dict):
    conditions = []
    params = []

    # 🎯 1. Обработка базовых фильтров
    conditions.append("price <= %s")
    params.append(filters["budget"])

    os_name = filters["osName"].lower()
    if os_name == "без ос":
        conditions.append("(os_name IS NULL OR os_name = 'без ОС')")
    elif os_name != "любая":
        conditions.append("os_name ILIKE %s")
        params.append(f"%{filters['osName']}%")

    if filters["caseMaterial"] == "металл":
        conditions.append("material IS DISTINCT FROM 'пластик' AND material IS NOT NULL")
    elif filters["caseMaterial"] == "пластик":
        conditions.append("material = 'пластик'")

    if "screenSize" in filters:
        min_size, max_size = filters["screenSize"]
        conditions.append("diagonal BETWEEN %s AND %s")
        params.extend((min_size, max_size))

    battery_time = filters["batteryTime"]
    conditions.append(f"battery_time {battery_time} OR battery_time IS NULL")

    if "weight" in filters:
        weight = filters["weight"]
        conditions.append("weight <= %s")
        params.append(weight)

    if "additionalFeatures" in filters:
        if "HDMI для вывода изображения на телевизор" in filters["additionalFeatures"]:
            conditions.append("video_slots ILIKE '%%HDMI%%'")
        if "Свободный слот для SSD накопителя" in filters["additionalFeatures"]:
            conditions.append("ssd_slots IS NOT NULL AND ssd_slots IS DISTINCT FROM 'нет'")
        if "Подсветка клавиш" in filters["additionalFeatures"]:
            conditions.append("kb_light IS DISTINCT FROM 'нет'")
        if "Сенсорный экран" in filters["additionalFeatures"]:
            conditions.append("touch_screen = 'есть'")

    # 📚 2. Объединение требований из use_cases
    requirements = {}
    
    case = filters.get("usageScenario", "")
    case_req = USE_CASE_REQUIREMENTS.get(case, {})
    for key, value in case_req.items():
        if key.startswith("min_"):
            requirements[key] = max(requirements.get(key, 0), value)
        elif key.startswith("max_"):
            requirements[key] = min(requirements.get(key, float("inf")), value)
        elif key == "gpu_type":
            requirements.setdefault("gpu_type", set()).update(value)
        elif key == "is_gaming_flag":
            requirements["is_gaming_flag"] = True

    # 💡 3. Превращаем требования в SQL-условия
    if "min_ram" in requirements:
        conditions.append("ram_size >= %s")
        params.append(requirements["min_ram"])

    if "min_p_cores" in requirements:
        conditions.append("p_cores >= %s")
        params.append(requirements["min_p_cores"])

    if "min_ssd" in requirements:
        conditions.append("ssd_size >= %s")
        params.append(requirements["min_ssd"])

    if "min_refresh_rate" in requirements:
        conditions.append("refresh_rate >= %s")
        params.append(requirements["min_refresh_rate"])

    if "min_battery" in requirements:
        conditions.append("battery_time >= %s")
        params.append(requirements["min_battery"])

    if "max_weight" in requirements:
        conditions.append("weight <= %s")
        params.append(requirements["max_weight"])

    if "gpu_type" in requirements:
        for gtype in requirements["gpu_type"]:
            if gtype == "gaming_discrete":
                conditions.append("gpu_discrete ILIKE ANY (ARRAY[%s, %s])")
                params.extend(['%RTX%', '%RX%'])
            elif gtype == "design":
                conditions.append("(cpu ILIKE ANY (ARRAY[%s, %s]) OR (gpu_discrete IS NOT NULL AND gpu_discrete IS DISTINCT FROM 'нет'))")
                params.extend(['%M3%', '%M4%'])
            
    if requirements.get("is_gaming_flag"):
        conditions.append("is_gaming = 1")

    # 🧾 4. Финальный SQL
    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    sql = f"""
        SELECT DISTINCT name, price, score FROM laptops
        {where_clause}
        ORDER BY score DESC, price ASC
        LIMIT 10
    """

    return sql.strip(), params
