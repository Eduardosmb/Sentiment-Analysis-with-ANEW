from fastapi import FastAPI, HTTPException, Query
from typing import List, Dict, Any
from pathlib import Path
from datetime import datetime
import requests
import csv
import time

app = FastAPI(title="Meu Back Rápido", version="0.1.0")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

def _timestamp():
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def save_comments_csv(
    subreddit: str,
    results: List[Dict[str, Any]],
) -> str:
    """
    Achata comentários (de todos os posts) e salva em CSV.
    results = lista com itens: { post: {...}, comments: [ {...}, ... ] }
    """
    fname = DATA_DIR / f"reddit_{subreddit}_comments_{_timestamp()}.csv"
    fields = [
        "subreddit",
        "post_id", "post_title", "post_index_in_hot", "post_permalink",
        "comment_id", "author", "body", "score", "created_utc", "parent_id", "is_submitter"
    ]
    with fname.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for item in results:
            post = item.get("post", {})
            for c in item.get("comments", []) or []:
                writer.writerow({
                    "subreddit": subreddit,
                    "post_id": post.get("id"),
                    "post_title": post.get("title"),
                    "post_index_in_hot": post.get("index_in_hot"),
                    "post_permalink": post.get("permalink"),
                    "comment_id": c.get("id"),
                    "author": c.get("author"),
                    "body": c.get("body"),
                    "score": c.get("score"),
                    "created_utc": c.get("created_utc"),
                    "parent_id": c.get("parent_id"),
                    "is_submitter": c.get("is_submitter"),
                })
    return str(fname)

@app.get("/")
def root():
    return {"message": "Salve Tiago"}

@app.get("/reddit/{subreddit}/hot-comments-to-csv")
def reddit_hot_comments_to_csv(
    subreddit: str,
    posts_limit: int = Query(5, ge=1, le=100, description="Qtd de posts da aba HOT (máx. 100)"),
    comments_limit: int = Query(200, ge=1, le=500, description="Qtd de comentários por post (máx. ~500)"),
    depth: int = Query(2, ge=1, le=10, description="Profundidade da árvore de comentários"),
    sort: str = Query("confidence", description="Ordenação: confidence|top|new|controversial|old|random|qa|live"),
    polite_delay_ms: int = Query(250, ge=0, le=2000, description="Atraso entre requisições (ms) para evitar rate limit"),
):
    """
    - Faz 1 requisição para listar os posts em /hot (até 100).
    - Para cada post, faz 1 requisição para comentários com limit alto (até ~500) e depth configurável.
    - NÃO expande 'MoreComments' (apenas a primeira leva devolvida pela API).
    - Salva todos os comentários coletados em CSV (./data/...csv).
    """
    HEADERS = {"User-Agent": "webcrawler/0.1 by Ceramel"}

    hot_url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit={posts_limit}"
    try:
        r = requests.get(hot_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except requests.HTTPError as e:
        raise HTTPException(status_code=r.status_code if 'r' in locals() else 500,
                            detail=f"Erro HTTP ao listar posts: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar posts: {e}")

    posts_json = r.json()
    children = posts_json.get("data", {}).get("children", [])
    if not children:
        raise HTTPException(status_code=404, detail="Nenhum post encontrado nesse subreddit.")

    results: List[Dict[str, Any]] = []

    for idx, child in enumerate(children, start=1):
        data = child.get("data", {})
        post_id = data.get("id")
        post_title = data.get("title")
        post_permalink = "https://www.reddit.com" + data.get("permalink", "")
        post_url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json"

        params = {
            "limit": comments_limit,
            "depth": depth,
            "sort": sort,
        }

        try:
            rc = requests.get(post_url, headers=HEADERS, params=params, timeout=25)
            rc.raise_for_status()
            comments_json = rc.json()
        except Exception as e:
            results.append({
                "post": {"id": post_id, "title": post_title, "permalink": post_permalink, "index_in_hot": idx},
                "comments": [],
                "error": f"Erro ao buscar comentários: {e}",
            })
            if polite_delay_ms:
                time.sleep(polite_delay_ms / 1000.0)
            continue

        if not isinstance(comments_json, list) or len(comments_json) < 2:
            results.append({
                "post": {"id": post_id, "title": post_title, "permalink": post_permalink, "index_in_hot": idx},
                "comments": [],
                "note": "Resposta inesperada da API de comentários.",
            })
            if polite_delay_ms:
                time.sleep(polite_delay_ms / 1000.0)
            continue

        comments_children = (
            comments_json[1]
            .get("data", {})
            .get("children", [])
        )

        extracted = []
        for c in comments_children:
            if c.get("kind") != "t1":
                continue
            cd = c.get("data", {})
            extracted.append({
                "id": cd.get("id"),
                "author": cd.get("author"),
                "body": cd.get("body"),
                "score": cd.get("score"),
                "created_utc": cd.get("created_utc"),
                "parent_id": cd.get("parent_id"),
                "is_submitter": cd.get("is_submitter"),
            })

        results.append({
            "post": {
                "id": post_id,
                "title": post_title,
                "permalink": post_permalink,
                "index_in_hot": idx,
            },
            "comments": extracted,
            "comments_count_returned": len(extracted),
            "note": "Somente a primeira leva; MoreComments não expandidos.",
        })

        if polite_delay_ms:
            time.sleep(polite_delay_ms / 1000.0)

    csv_path = save_comments_csv(subreddit, results)

    return {
        "subreddit": subreddit,
        "posts_processed": len(results),
        "comments_total_returned": sum(len(x.get("comments", [])) for x in results),
        "csv_saved_to": csv_path,
        "limits_reminder": {
            "max_posts_per_hot_request": 100,
            "max_comments_per_post_request": "~500 (primeira leva; pode haver MoreComments)",
        },
    }
