from app.data_builder import main as build_feed
from app.main import run_pipeline

if __name__ == '__main__':
    build_feed()
    run_pipeline()
