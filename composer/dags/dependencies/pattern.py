
def get_pattern_timestamp(pattern):
    pattern_upper = pattern.upper()
    if pattern_upper == '5MIN':
        return ">= FORMAT_TIMESTAMP('%F %T', TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 10 MINUTE), 'Europe/Paris')"

def get_pattern_date(pattern):
    pattern_upper = pattern.upper()
    if pattern_upper == 'LAST_24H':
        return ">= DATE(TIMESTAMP(FORMAT_TIMESTAMP('%F %T', CURRENT_TIMESTAMP, 'Europe/Paris')))"
    elif pattern_upper == '5MIN':
        return ">= DATE(TIMESTAMP(FORMAT_TIMESTAMP('%F %T', CURRENT_TIMESTAMP, 'Europe/Paris')))"

def get_pattern_full(pattern):
    pattern_upper = pattern.upper()
    if pattern_upper == '5MIN':
        return ""

def get_pattern_date_long(pattern):
    pattern_upper = pattern.upper()
    if pattern_upper == '5MIN':
        return ">= DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH)"