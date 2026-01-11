#!/bin/bash

# Полное обновление NDFL сервера включая Docker контейнеры
# Использование: bash scripts/full_update.sh

# Строгий режим: останавливаем выполнение при любой ошибке
set -euo pipefail

# Обработчик ошибок
trap 'echo -e "${RED}❌ Ошибка на строке $LINENO. Обновление прервано!${NC}"; exit 1' ERR

# Цвета для вывода
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         🚀 ПОЛНОЕ ОБНОВЛЕНИЕ NDFL                        ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

# Проверяем, что мы в правильной директории
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}❌ Ошибка: docker-compose.yml не найден!${NC}"
    echo -e "${RED}   Убедитесь, что вы в директории /home/batman/ndfl${NC}"
    exit 1
fi

echo -e "${YELLOW}📍 Текущая директория: $(pwd)${NC}"
echo ""

# Шаг 1: Остановка контейнеров
echo -e "${YELLOW}[1/8] 🛑 Останавливаю Docker контейнеры...${NC}"
docker-compose down
echo -e "${GREEN}✓ Контейнеры остановлены${NC}"
echo ""

# Шаг 2: Очистка Docker
echo -e "${YELLOW}[2/8] 🧹 Очищаю Docker систему...${NC}"
# Удаляем старые образы ndfl
echo -e "${YELLOW}  Удаляю старые образы ndfl...${NC}"
OLD_IMAGES=$(docker images -q 'ndfl*' 2>/dev/null || true)
if [ -n "$OLD_IMAGES" ]; then
    docker rmi $OLD_IMAGES 2>/dev/null || true
    echo -e "${GREEN}  ✓ Удалены старые образы${NC}"
else
    echo -e "${YELLOW}  ℹ Старых образов не найдено${NC}"
fi
# Очищаем ТОЛЬКО контейнеры и сети, НЕ удаляем базовые образы (postgres)
docker container prune -f 2>/dev/null || true
docker network prune -f 2>/dev/null || true
echo -e "${GREEN}✓ Docker очищен (базовые образы сохранены)${NC}"
echo ""

# Шаг 3: Получение изменений из Git
echo -e "${YELLOW}[3/8] 📥 Получаю последние изменения из Git...${NC}"
git fetch --all
git reset --hard origin/master
git pull origin master
echo -e "${GREEN}✓ Код обновлен из репозитория${NC}"
echo ""

# Шаг 4: Пересборка Docker образов
echo -e "${YELLOW}[4/8] 🔨 Пересобираю Docker образы...${NC}"
docker-compose build --no-cache
echo -e "${GREEN}✓ Docker образы пересобраны${NC}"
echo ""

# Шаг 5: Очистка Docker build cache
echo -e "${YELLOW}[5/8] 🧹 Очищаю Docker build cache...${NC}"
CACHE_SIZE=$(docker builder prune -f 2>&1 | grep "Total:" | awk '{print $2}' || echo "0B")
if [ "$CACHE_SIZE" != "0B" ] && [ -n "$CACHE_SIZE" ]; then
    echo -e "${GREEN}✓ Очищено build cache: $CACHE_SIZE${NC}"
else
    echo -e "${GREEN}✓ Build cache пуст или очищен${NC}"
fi
echo ""

# Шаг 6: Запуск новых контейнеров
echo -e "${YELLOW}[6/8] 🚀 Запускаю новые контейнеры...${NC}"
docker-compose up -d --force-recreate
echo -e "${GREEN}✓ Контейнеры запущены${NC}"
echo ""

# Шаг 7: Применение миграций Django
echo -e "${YELLOW}[7/8] 📊 Применяю миграции Django...${NC}"
echo -e "${YELLOW}  Жду готовности базы данных (15 сек)...${NC}"
sleep 15

# Применяем миграции
docker exec ndfl_web python manage.py migrate --noinput
echo -e "${GREEN}✓ Миграции применены${NC}"

# Собираем статику
echo -e "${YELLOW}  Собираю статические файлы...${NC}"
docker exec ndfl_web python manage.py collectstatic --noinput
echo -e "${GREEN}✓ Статика собрана${NC}"
echo ""

# Шаг 8: Финальная проверка
echo -e "${YELLOW}[8/8] 🔍 Выполняю финальные проверки...${NC}"

# Проверка что контейнеры запущены
docker-compose ps
echo ""

RUNNING_CONTAINERS=$(docker-compose ps | grep "Up" | wc -l)
EXPECTED_CONTAINERS=2  # web, db

if [ $RUNNING_CONTAINERS -ge $EXPECTED_CONTAINERS ]; then
    echo -e "${GREEN}✓ Все контейнеры запущены ($RUNNING_CONTAINERS)${NC}"
else
    echo -e "${YELLOW}⚠️ Запущено контейнеров: $RUNNING_CONTAINERS из $EXPECTED_CONTAINERS ожидаемых${NC}"
fi

# Проверка доступности сайта
set +e
echo ""
echo -e "${YELLOW}🌐 Проверяю доступность сайта...${NC}"

# Проверка главной страницы
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" https://ndfl.duckdns.org/ 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "301" ] || [ "$HTTP_CODE" = "302" ]; then
    echo -e "${GREEN}✓ Сайт доступен: https://ndfl.duckdns.org/ (HTTP $HTTP_CODE)${NC}"
else
    echo -e "${YELLOW}⚠️ Сайт может быть недоступен (HTTP $HTTP_CODE)${NC}"
fi

# Проверка админки
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" https://ndfl.duckdns.org/admin/ 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "301" ] || [ "$HTTP_CODE" = "302" ]; then
    echo -e "${GREEN}✓ Админка доступна: https://ndfl.duckdns.org/admin/ (HTTP $HTTP_CODE)${NC}"
else
    echo -e "${YELLOW}⚠️ Админка может быть недоступна (HTTP $HTTP_CODE)${NC}"
fi
set -e

echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         ✅ ОБНОВЛЕНИЕ ЗАВЕРШЕНО УСПЕШНО!                 ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}📌 Что проверить:${NC}"
echo -e "   1. Сайт: https://ndfl.duckdns.org/"
echo -e "   2. Админка: https://ndfl.duckdns.org/admin/"
echo ""
echo -e "${YELLOW}💡 Совет: Если что-то не работает, проверьте логи:${NC}"
echo -e "   docker-compose logs -f web"
echo -e "   docker-compose logs -f db"
echo ""
