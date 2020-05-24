from celery import Celery
from start import start


# Для запуска воркера по расписанию: celery -A tasks worker --beat
# Для остановки использовать команду: kill -- <PID из файла>

app = Celery(
    main="tasks",
    # backend="redis://localhost:6379",
    broker="redis://redis"
)

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Europe/Moscow',
    enable_utc=True,
    beat_schedule={
        'periodic_task': {
            'task': 'tasks.parser_daemon',
            'schedule': 1500.0,
            # 'args': (16, 16)
        }
    }
)


@app.task()
def parser_daemon():
    start()
