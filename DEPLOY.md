# Инструкция по деплою NDFL

## Сервер
- **IP:** 176.124.218.53
- **User:** batman
- **Домен:** ndfl.duckdns.org
- **Путь:** /home/batman/ndfl

---

## Шаг 1: Подключение к серверу

```bash
ssh batman@176.124.218.53
```

---

## Шаг 2: Клонирование репозитория

```bash
cd /home/batman
git clone https://github.com/sananlb/NDFL.git ndfl
cd ndfl
```

---

## Шаг 3: Создание .env файла

```bash
cp .env.example .env
nano .env
```

Заполните:
```
SECRET_KEY=сгенерируйте-случайный-ключ-64-символа
DEBUG=False
ALLOWED_HOSTS=ndfl.duckdns.org,localhost,127.0.0.1
DB_NAME=ndfl
DB_USER=ndfl_user
DB_PASSWORD=ваш-надежный-пароль
DB_HOST=db
DB_PORT=5432
CSRF_TRUSTED_ORIGINS=https://ndfl.duckdns.org
```

Для генерации SECRET_KEY:
```bash
python3 -c "import secrets; print(secrets.token_urlsafe(50))"
```

---

## Шаг 4: Сборка и запуск Docker

```bash
cd /home/batman/ndfl
docker-compose build
docker-compose up -d
```

---

## Шаг 5: Миграции и superuser

```bash
# Применить миграции
docker-compose exec web python manage.py migrate

# Создать суперпользователя
docker-compose exec web python manage.py createsuperuser

# Собрать статику (если не собралась при build)
docker-compose exec web python manage.py collectstatic --noinput
```

---

## Шаг 6: Настройка Nginx

```bash
# Скопировать конфиг
sudo cp /home/batman/ndfl/nginx/ndfl-ssl.conf /etc/nginx/sites-available/ndfl

# Временно создать конфиг без SSL для получения сертификата
sudo nano /etc/nginx/sites-available/ndfl
```

Временный конфиг (без SSL):
```nginx
server {
    listen 80;
    server_name ndfl.duckdns.org;

    location / {
        proxy_pass http://localhost:8010;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /static/ {
        alias /home/batman/ndfl/staticfiles/;
    }

    location /media/ {
        alias /home/batman/ndfl/media/;
    }
}
```

```bash
# Включить сайт
sudo ln -s /etc/nginx/sites-available/ndfl /etc/nginx/sites-enabled/

# Проверить конфиг
sudo nginx -t

# Перезагрузить nginx
sudo nginx -s reload
```

---

## Шаг 7: Получение SSL сертификата

```bash
sudo certbot --nginx -d ndfl.duckdns.org
```

После получения сертификата, замените конфиг на полный (с SSL):
```bash
sudo cp /home/batman/ndfl/nginx/ndfl-ssl.conf /etc/nginx/sites-available/ndfl
sudo nginx -s reload
```

---

## Шаг 8: Создание папки для статики на хосте

```bash
mkdir -p /home/batman/ndfl/staticfiles
mkdir -p /home/batman/ndfl/media

# Скопировать статику из контейнера
docker cp ndfl_web:/app/staticfiles/. /home/batman/ndfl/staticfiles/
```

---

## Проверка

```bash
# Статус контейнеров
docker-compose ps

# Логи
docker-compose logs -f web

# Проверить сайт
curl -I https://ndfl.duckdns.org
```

---

## Полезные команды

```bash
# Перезапуск
cd /home/batman/ndfl && docker-compose restart

# Остановка
cd /home/batman/ndfl && docker-compose down

# Обновление кода
cd /home/batman/ndfl && git pull && docker-compose build && docker-compose up -d

# Логи Django
cd /home/batman/ndfl && docker-compose logs --tail=100 web

# Вход в контейнер
docker-compose exec web bash
```

---

## Архитектура на сервере

```
176.124.218.53
├── expense_bot (порт 8000) → expensebot.duckdns.org
└── ndfl (порт 8010) → ndfl.duckdns.org
```
