import html
import os

from pipeline.settings import DQ_REPORTS_DIR


def write_report(check_info: dict) -> dict:
    """Читает результаты DQ, пишет HTML; повтор атомарно заменяет отчёт."""
    DQ_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = DQ_REPORTS_DIR / "latest.html"
    rows = "".join(
        f"<tr><td>{html.escape(item['rule'])}</td><td>{'PASS' if item['passed'] else 'FAIL'}</td><td>{item['violations']}</td></tr>"
        for item in check_info["checks"]
    )
    status = "PASS" if check_info["passed"] else "FAIL"
    content = f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<title>Data Quality — {status}</title></head><body>
<h1>Data Quality: {status}</h1>
<p>Запуск: {html.escape(check_info['run_id'])}; строк: {check_info['rows']}</p>
<table border="1"><tr><th>Правило</th><th>Результат</th><th>Нарушения</th></tr>{rows}</table>
</body></html>"""
    temporary = DQ_REPORTS_DIR / f".{check_info['report_id']}.tmp"
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, report)
    return {"passed": check_info["passed"], "report_path": str(report)}


def assert_quality(report_info: dict) -> str:
    """Читает итог DQ, выставляет статус после записи HTML; данные не меняет."""
    if not report_info["passed"]:
        raise ValueError(f"Data Quality failed: {report_info['report_path']}")
    return report_info["report_path"]
