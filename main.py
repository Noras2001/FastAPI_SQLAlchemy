from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import uvicorn
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool




from sqlalchemy import (
    create_engine, Column, Integer, String, ForeignKey,
    DateTime, Float, func, select, MetaData, Table
)
from sqlalchemy.orm import (
    declarative_base, relationship, sessionmaker,
    joinedload, Session as SessionType
)
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

# ============================================================
# 1. Настройка SQLAlchemy: модели, движок, фабрика сессий
# ============================================================

# Базовый класс для декларативных моделей
Base = declarative_base()

# Модель Author для демонстрации ленивой и жадной загрузки (Задача 1)
class Author(Base):
    __tablename__ = 'authors'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    # Отношение "один ко многим": один автор может иметь много книг.
    # По умолчанию используется lazy load 
    books = relationship("Book", back_populates="author", lazy="select")

# Модель Book для демонстрации ленивой/жадной загрузки и паттерна Repository (Задача 1 и 4)
class Book(Base):
    __tablename__ = 'books'
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    author_id = Column(Integer, ForeignKey("authors.id"))
    author = relationship("Author", back_populates="books")

# Модель User для демонстрации транзакций и отката (Задача 2)
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False)
    email = Column(String, nullable=False)

# Модель Order для демонстрации миграций с Alembic (Задача 3)
# Первоначальное состояние таблицы (поля: id, product_name, quantity, created_at)
class Order(Base):
    __tablename__ = 'orders'
    id = Column(Integer, primary_key=True)
    product_name = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

# Создаем движок с включенным логированием SQL-запросов (echo=True)
#engine = create_engine("sqlite:///:memory:", echo=True, future=True)
engine = create_engine(
    "sqlite:///:memory:",
    echo=True,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool
)

# Создаем все таблицы
Base.metadata.create_all(engine)

# Фабрика сессий
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, class_=SessionType)

# ============================================================
# 2. Реализация паттерна Repository для модели Book (Задача 4)
# ============================================================
class BookRepository:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    def add_book(self, title: str, author_id: int) -> Book:
        session = self.session_factory()
        try:
            new_book = Book(title=title, author_id=author_id)
            session.add(new_book)
            session.commit()
            session.refresh(new_book)
            return new_book
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_books_by_author(self, author_id: int) -> List[Book]:
        session = self.session_factory()
        try:
            books = session.query(Book).filter(Book.author_id == author_id).all()
            return books
        finally:
            session.close()

    def delete_book(self, book_id: int) -> bool:
        session = self.session_factory()
        try:
            book = session.get(Book, book_id)
            if book:
                session.delete(book)
                session.commit()
                return True
            return False
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

# Создаем экземпляр репозитория для использования в API
book_repo = BookRepository(SessionLocal)

# ============================================================
# 3. Определение Pydantic схем для FastAPI
# ============================================================
class BookCreate(BaseModel):
    title: str
    author_id: int

class BookOut(BaseModel):
    id: int
    title: str
    author_id: int
    class Config:
        orm_mode = True

class UserCreate(BaseModel):
    username: str
    email: str

# ============================================================
# 4. Создание приложения FastAPI и маршрутов (endpoints)
# ============================================================
app = FastAPI(title="FastAPI app using SQLAlchemy")

# ---------------------------
# Инициализация тестовых данных
# ---------------------------
@app.on_event("startup")
def startup_event():
    # Добавляем авторов и книги для демонстрации ленивой/жадной загрузки
    session = SessionLocal()
    try:
        # Создаем автора с книгами
        author = Author(name="Ахматова")
        author.books = [Book(title="Реквием"), Book(title="Поэма без героя")]
        session.add(author)
        session.commit()
    finally:
        session.close()

# ---------------------------
# Задача 1: Демонстрация ленивой и жадной загрузки
# ---------------------------
@app.get("/authors/lazy")
def get_authors_lazy():
    """
    Получаем авторов с ленивой загрузкой (по умолчанию).
    При обращении к связанным книгам будут выполняться дополнительные SQL-запросы (N+1).
    """
    session = SessionLocal()
    try:
        authors = session.query(Author).all()
        # Для каждого автора при обращении к author.books выполняется отдельный запрос
        result = [{"id": a.id, "name": a.name, "books": [b.title for b in a.books]} for a in authors]
        return result
    finally:
        session.close()

@app.get("/authors/eager")
def get_authors_eager():
    """
    Получаем авторов с жадной загрузкой (eager) с использованием joinedload.
    Связанные книги загружаются одним JOIN-запросом.
    """
    session = SessionLocal()
    try:
        authors = session.query(Author).options(joinedload(Author.books)).all()
        result = [{"id": a.id, "name": a.name, "books": [b.title for b in a.books]} for a in authors]
        return result
    finally:
        session.close()

# ---------------------------
# Задача 2: Демонстрация транзакций и откатов изменений
# ---------------------------
@app.post("/users/transaction")
def create_users_transaction():
    """
    Создает двух пользователей, а затем имитирует ошибку при добавлении третьего.
    В случае ошибки происходит откат транзакции.
    """
    session = SessionLocal()
    try:
        # Начинаем транзакцию
        with session.begin():
            user1 = User(username="user1", email="user1@example.com")
            session.add(user1)
            user2 = User(username="user2", email="user2@example.com")
            session.add(user2)
            # Имитация ошибки
            raise Exception("Искусственная ошибка при добавлении третьего пользователя")
            user3 = User(username="user3", email="user3@example.com")
            session.add(user3)
    except Exception as e:
        # Транзакция откатывается автоматически
        return {"status": "error", "message": f"Транзакция откатилась: {e}"}
    finally:
        session.close()

# ---------------------------
# Задача 3: Демонстрация работы Alembic (инструкции)
# ---------------------------
@app.get("/alembic/instructions")
def alembic_instructions():
    """
    Данный эндпоинт содержит инструкции по работе с Alembic.
    1. Инициализация Alembic:
       alembic init alembic
    2. Создание миграции для создания таблицы Order:
       alembic revision -m "create order table"
       (В файле миграции опишите создание таблицы с полями id, product_name, quantity, created_at)
    3. Применение миграции:
       alembic upgrade head
    4. Изменение структуры таблицы:
       alembic revision -m "alter order table: add price, drop created_at"
       (Добавьте поле price, удалите created_at)
    5. Откат последней миграции:
       alembic downgrade -1
    """
    return {"instructions": "Смотрите комментарии в коде для подробных шагов работы с Alembic."}

# ---------------------------
# Задача 4: Демонстрация паттерна Repository для модели Book
# ---------------------------
@app.post("/books/add", response_model=BookOut)
def add_book(book: BookCreate):
    """
    Добавление новой книги через репозиторий.
    """
    try:
        new_book = book_repo.add_book(book.title, book.author_id)
        return new_book
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/books/by_author/{author_id}", response_model=List[BookOut])
def get_books_by_author(author_id: int):
    """
    Получение всех книг заданного автора.
    """
    try:
        books = book_repo.get_books_by_author(author_id)
        return books
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/books/{book_id}")
def delete_book(book_id: int):
    """
    Удаление книги по её id.
    """
    try:
        result = book_repo.delete_book(book_id)
        if result:
            return {"status": "success", "message": f"Книга с id {book_id} удалена"}
        else:
            raise HTTPException(status_code=404, detail="Книга не найдена")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ---------------------------
# Задача 5: Логирование и комбинирование ORM и Core
# ---------------------------
@app.get("/authors/book_counts")
def get_authors_book_counts():
    """
    Сложный агрегирующий запрос с использованием SQLAlchemy Core.
    Выводит количество книг для каждого автора.
    """
    # Создаем объект MetaData и загружаем таблицы
    metadata = MetaData(bind=engine)
    authors_table = Table("authors", metadata, autoload_with=engine)
    books_table = Table("books", metadata, autoload_with=engine)
    
    # Составляем агрегирующий запрос
    stmt = (
        select(
            authors_table.c.name,
            func.count(books_table.c.id).label("book_count")
        )
        .select_from(authors_table.join(books_table, authors_table.c.id == books_table.c.author_id))
        .group_by(authors_table.c.name)
    )
    # Выполнение запроса через Core
    with engine.connect() as connection:
        result = connection.execute(stmt)
        counts = [{"author": row["name"], "book_count": row["book_count"]} for row in result]
    return counts

# ============================================================
# Запуск приложения через uvicorn
# ============================================================
if __name__ == "__main__":
    # Запуск приложения: uvicorn main:app --reload
    uvicorn.run(app, host="127.0.0.1", port=8000)
