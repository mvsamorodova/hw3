import os

import pandas as pd

from pipeline.settings import CANDIDATES_DIR, CAT_FEATURES, LABEL


def prepare_features(frame):
    features = frame.drop(columns=[LABEL]).copy()
    for column in CAT_FEATURES:
        features[column] = features[column].astype(str)
    return features, frame[LABEL]


def fit_catboost(preproc_info: dict) -> dict:
    """Читает train, сохраняет кандидата; повтор использует готовую модель."""
    from catboost import CatBoostClassifier

    CANDIDATES_DIR.mkdir(parents=True, exist_ok=True)
    version = preproc_info["version"]
    path = CANDIDATES_DIR / f"my_catboost_{version}.cbm"
    if path.exists():
        return {**preproc_info, "candidate_model": str(path)}
    train = pd.read_parquet(preproc_info["train_proc_path"])
    x_train, y_train = prepare_features(train)
    model = CatBoostClassifier(
        iterations=100, depth=6, learning_rate=0.1,
        loss_function="Logloss", auto_class_weights="Balanced", random_seed=42,
        task_type="CPU", thread_count=2, allow_writing_files=False, verbose=False,
    )
    model.fit(x_train, y_train, cat_features=CAT_FEATURES)
    temporary = path.with_suffix(".tmp.cbm")
    model.save_model(str(temporary))
    os.replace(temporary, path)
    return {**preproc_info, "candidate_model": str(path)}
