import time
import requests
import argparse
from typing import Optional
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

API_BASE_URL = "https://wikimedia.org/api/rest_v1/metrics"
TOP_ENDPOINT = "pageviews/top"
TOP_ARGS = "{project}/{access}/{year}/{month}/{day}"


def get_top_wiki_articles(project: str, year: str, month: str, day: str,
                          access: str = "all-access", retries: int = 3, delay: int = 1) -> Optional[dict]:

    args = TOP_ARGS.format(project=project,
                           access=access,
                           year=year,
                           month=month,
                           day=day)

    return __api__(TOP_ENDPOINT, args, retries=retries, delay=delay)


def __api__(end_point: str, args: str, api_url=API_BASE_URL,
            retries: int = 3, delay: int = 1) -> Optional[dict]:
    """
    Изменения:
      - Добавила механизм повторных попыток (retry) и задержку между попытками (delay)
      - Возвращаемый тип изменила на Optional[dict]
      - Добавила логирование ошибок
    """
    url = "/".join([api_url, end_point, args])
    for attempt in range(retries):
        try:
            response = requests.get(url, headers={"User-Agent": "wiki_analyzer"})
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    print(f"Не удалось получить данные после {retries} попыток: {url}")
    return None


def main(start_date: dt.datetime, end_date: dt.datetime) -> None:
    """
    Изменения:
      - Добавила форматирование дат
      - Добавила проверки на пустые и невалидные данные
      - Добавила логирование ошибок
      - Изменила логику сбора и обработки данных:
            - убрала конкатенацию в цикле
            - добавила фильтрацию топ-20 до обработки данных
            - добавила логику создания временного ряда для каждой статьи отдельно
            - изменила расчет mean_views (среднее по всем статьям от средних по каждой)
    """

    dates = pd.date_range(start_date, end_date, freq='D')
    data_frames = []

    for date in dates:
        date_str = date.strftime("%Y%m%d")
        data = get_top_wiki_articles(
            "en.wikipedia",
            date.strftime('%Y'),
            date.strftime('%m'),
            date.strftime('%d')
        )  # форматируем даты, чтобы для месяца и дня гарантированно получать двузначный формат (для года - 4-значный)
        if not data or "items" not in data:
            print(f"Пропуск {date_str} - нет валидных данных")
            continue

        df_day = pd.DataFrame(data["items"][0]["articles"])
        df_day["date"] = date
        data_frames.append(df_day)
        time.sleep(0.1)  # задержка для соблюдения лимитов API

    if not data_frames:
        raise ValueError("Данные для заданного диапазона дат не собраны")

    df = pd.concat(data_frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])

    last_values = df.sort_values("date").groupby("article")["views"].last()
    top_articles = last_values.nlargest(20).index.tolist()
    df_top = df[df["article"].isin(top_articles)]  # сразу отбираем топ-20 статей

    date_range = pd.date_range(start_date, end_date, name="date")
    article_dfs = []

    # создаем временной ряд и обработываем NaN для каждой статьи отдельно
    for article in top_articles:
        article_df = df_top[df_top["article"] == article]
        article_full = pd.DataFrame({"date": date_range})
        article_full = article_full.merge(article_df[["date", "views"]], on="date", how="left")
        article_full["views"] = article_full["views"].ffill()
        article_full["article"] = article
        article_dfs.append(article_full)

    df_plot = pd.concat(article_dfs, ignore_index=True)

    valid_views = df_top.dropna(subset=["views"])
    if valid_views.empty:
        raise ValueError("Нет данных по количеству просмотров")

    # статистики
    mean_views_by_article = valid_views.groupby("article")["views"].mean() # среднее по каждой статье
    mean_views = mean_views_by_article.mean() # общее среднее по всем статьям (от средних по каждой)
    max_views = valid_views["views"].max()
    unique_articles = df["article"].nunique()

    title = f"Top articles wiki views (Mean: {mean_views:.2f}, Max: {max_views}, Articles: {unique_articles})"

    plt.figure(figsize=(14, 8))
    for article, group in df_plot.groupby("article"):
        plt.plot(group["date"], group["views"], label=article, linewidth=2)

    plt.title(title, fontsize=16)
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Views (log scale)", fontsize=12)
    plt.yscale("log")
    plt.legend(loc="upper left")
    plt.tight_layout()
    plt.savefig("Exercise-1/top_articles.png", dpi=300)
    plt.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wikipedia top articles view analyzer")
    parser.add_argument("start", type=str, help="Start date in YYYYMMDD format")
    parser.add_argument("end", type=str, help="End date in YYYYMMDD format")
    args = parser.parse_args()

    try:
        start_date = dt.datetime.strptime(args.start, "%Y%m%d")
        end_date = dt.datetime.strptime(args.end, "%Y%m%d")
    except ValueError as e:
        raise SystemExit(f"Неправильный формат даты: {e}") from e

    if end_date < start_date:
        raise SystemExit("Дата окончания должна быть после даты начала")

    main(start_date, end_date)