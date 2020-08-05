from flask_caching import Cache

from app1 import app, CACHE_CONFIG

cache = Cache()


def main():
    cache.init_app(app, config=CACHE_CONFIG)

    with app.app_context():
        cache.clear()

if __name__ == '__main__':
    main()