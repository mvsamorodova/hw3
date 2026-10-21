import json

import pandas as pd

from pipeline.db import get_conn
from pipeline.retrain.fit_catboost import prepare_features
from pipeline.settings import MODELS_DIR


def current_version():
    path = MODELS_DIR / "latest.txt"
    return path.read_text(encoding="utf-8").strip() if path.exists() else None


def model_metrics(path, features, target):
    from catboost import CatBoostClassifier
    from sklearn.metrics import f1_score, roc_auc_score

    model = CatBoostClassifier()
    model.load_model(str(path))
    values = features.loc[:, model.feature_names_].copy()
    for index in model.get_cat_feature_indices():
        column = model.feature_names_[index]
        if not pd.api.types.is_numeric_dtype(values[column]):
            values[column] = values[column].astype(str)
    probability = model.predict_proba(values, thread_count=2)[:, 1]
    return {
        "roc_auc": float(roc_auc_score(target, probability)),
        "f1": float(f1_score(target, probability > 0.5, zero_division=0)),
    }


def evaluate_models(train_info: dict) -> dict:
    """Читает модели и test, пишет решение в БД; повтор учитывает статус и рабочую версию."""
    version = current_version()
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status, decision FROM training_runs WHERE run_id = %s", (train_info["run_id"],))
                existing = cur.fetchone()
                if existing:
                    status, decision = existing
                    if status in ("published", "rejected"):
                        return decision
                    if version == decision["incumbent_version"] or (
                        decision["accepted"] and version == decision["version"]
                    ):
                        return decision

                test = pd.read_parquet(train_info["test_proc_path"])
                features, target = prepare_features(test)
                candidate = model_metrics(train_info["candidate_model"], features, target)
                incumbent = model_metrics(MODELS_DIR / f"my_catboost_{version}.cbm", features, target) if version else None
                accepted = incumbent is None or (
                    all(candidate[metric] >= incumbent[metric] for metric in candidate)
                    and any(candidate[metric] > incumbent[metric] for metric in candidate)
                )
                info = {
                    **train_info,
                    "incumbent_version": version,
                    "candidate_metrics": candidate,
                    "incumbent_metrics": incumbent,
                    "accepted": accepted,
                }
                cur.execute("""
                    INSERT INTO training_runs (run_id, batch_month, version, status, decision)
                    VALUES (%s, %s, %s, 'evaluated', %s::jsonb)
                    ON CONFLICT (run_id) DO UPDATE SET decision = EXCLUDED.decision
                    WHERE training_runs.status = 'evaluated'
                """, (info["run_id"], info["batch_month"], info["version"], json.dumps(info, allow_nan=False)))
        return info
    finally:
        conn.close()
