from news_parser.common.db_manager import DB, DBParamsDTO
from news_parser.common.regexp_template import RegExp
from news_parser.settings import DB_PARAMS
from typing import NamedTuple, Union, Optional
from datetime import datetime
from openpyxl import Workbook as excel_wb
import json


class ReportException(Exception):
    pass


class RPostAutoParsingDTO(NamedTuple):
    datetime_begin: datetime
    datetime_end: Optional[datetime]


class RParamsDTO(NamedTuple):
    main: Union[RPostAutoParsingDTO]
    recipient_id: int


class Report:
    def __init__(self, params: Optional[RParamsDTO] = None):
        self.db = DB(DBParamsDTO(
            host=DB_PARAMS["host"],
            port=DB_PARAMS["port"],
            name=DB_PARAMS["name"],
            user=DB_PARAMS["user"],
            password=DB_PARAMS["password"]
        ))
        self.params: Optional[RParamsDTO] = None
        if params is not None:
            self.set_params(params)

    def set_params(self, params: RParamsDTO):
        self.params = params

    def _fixate_event(self):
        pass

    def generate_report(self):
        # self._fixate_event()

        if type(self.params.main) is RPostAutoParsingDTO:
            worker = ReportPostAutoParsing(self.db, self.params.main)
        else:
            raise ReportException("Report._parse_run_params.1: Не задан режим генерации отчета")

        fname = worker.make_report_file()
        recipients = worker.get_recipients()

        return fname, recipients


class ReportPostAutoParsing:
    type_code = "report_auto_parsing"

    def __init__(self, db: DB, params: RPostAutoParsingDTO):
        self.db = db
        self.params = params
        self.type_id = self._fill_type_id()

    def _fill_type_id(self):
        self.db.cur.execute(
            RegExp.space.sub(" ", """
                SELECT id
                  FROM ref_type
                 WHERE code = %(p_type_code)s
            """),
            {"p_type_code": self.type_code}
        )
        return self.db.cur.fetchone()[0]

    def _get_posts(self):
        self.db.cur.execute(
            RegExp.space.sub(" ", """
                WITH RECURSIVE grouped_posts as (
                  SELECT 1 as level
                       , tpi.id as group_base_id
                       , tpi.datetime as post_created
                       , tpi.*
                    FROM t_post_info tpi
                   WHERE parent_id IS NULL
                   UNION ALL
                  SELECT gp.level + 1 as level
                       , gp.group_base_id
                       , gp.post_created
                       , tpi.*
                    FROM t_post_info tpi
                   INNER JOIN grouped_posts gp
                         ON tpi.parent_id = gp.id
                )
                SELECT vt1.id
                     , vt1.source_name
                     , vt1.login
                     , vt1.url
                     , vt1.title
                     , CASE WHEN vt1.type_code = 'post' THEN vt1.message END as message
                     , CASE WHEN vt1.type_code = 'comment' THEN vt1.message END as comment
                     , vt1.bank_answer
                     , vt1.rating
                     , to_char(vt1.datetime, 'DD.MM.YYYY HH24:MI:SS') as datetime
                  FROM (SELECT gp.id
                             , rs.name as source_name
                             , rt.code as type_code
                             , ra.login
                             , gp.content ->> 'post_url' as url
                             , gp.content ->> 'title' as title
                             , regexp_replace(gp.content ->> 'msg', '(\d){4}((\s)|[-\.:])*(\d){4}((\s)|[-\.:])*(\d){4}((\s)|[-\.:])*(\d){4}', '') as message
                             , gp.content ->> 'bank_answer' as bank_answer
                             , gp.content ->> 'rating' as rating
                             , gp.datetime
                          FROM grouped_posts gp
                         INNER JOIN (SELECT max(tbl2.post_id) as last_post_id
                                       FROM (SELECT unnest(tr.ref_posts) as post_id
                                               FROM t_report tr
                                              INNER JOIN (SELECT max(tr.id) as last_id
                                                            FROM t_report tr
                                                           WHERE     tr.ref_type = %(p_type_id)s
                                                                 AND cardinality(tr.ref_posts) > 0) tbl1
                                                    ON tr.id = tbl1.last_id) tbl2) tbl3
                               ON gp.id > coalesce(tbl3.last_post_id, 0)
                         INNER JOIN ref_account ra
                               ON gp.ref_account = ra.id
                         INNER JOIN ref_type rt
                               ON gp.ref_type = rt.id
                         INNER JOIN ref_source rs
                               ON gp.ref_source = rs.id
                         ORDER BY rs.name, gp.post_created, gp.group_base_id, gp.level, gp.datetime) vt1
            """),
            {"p_type_id": self.type_id}
        )

        while (row := self.db.cur.fetchone()) is not None:
            yield row

    def _save_report_info(self, ids: list):
        self.db.cur.execute(
            RegExp.space.sub(" ", """
                INSERT INTO t_report(ref_type, parameters, ref_posts)
                VALUES (
                  %(p_type_id)s,
                  %(p_params)s,
                  %(p_ids)s
                )
            """),
            {
                "p_type_id": self.type_id,
                "p_params": json.dumps({}),
                "p_ids": ids
            }
        )
        self.db.commit()

    def make_report_file(self):
        current_date = datetime.now()

        wb = excel_wb()
        sheet = wb.active
        sheet.title = f"Отчет за {current_date.strftime('%d.%m.%Y %H-%M-%S')}"
        header = [
            "id", "Ресурс", "Пользователь", "URL", "Заголовок", "Сообщение",
            "Комментарий", "Ответ Банка", "Рейтинг", "Дата"
        ]

        nrow = 1
        for i, val in enumerate(header, 1):
            sheet.cell(nrow, i).value = val

        ids = list()
        gr = self._get_posts()
        for row in gr:
            nrow += 1
            ids.append(row[0])
            for i, val in enumerate(row, 1):
                sheet.cell(nrow, i).value = val

        fname = f"{self.type_code}_{current_date.strftime('%Y%m%d%H%M%S')}.xlsx"
        wb.save(fname)
        wb.close()

        self._save_report_info(ids)

        return fname

    def get_recipients(self):
        self.db.cur.execute(
            RegExp.space.sub(" ", """
                SELECT string_agg(ra.login, ';') as accounts_str
                  FROM (SELECT unnest(ral.ref_accounts) as account_id
                          FROM ref_account_list ral
                         INNER JOIN ref_type tr
                               ON ral.ref_type = tr.id
                               AND tr.code = 'report_recipient_list'
                         WHERE ral.actual IS TRUE) vt1
                 INNER JOIN ref_account ra
                       ON vt1.account_id = ra.id
            """)
        )
        return (self.db.cur.fetchone() or [None])[0]

    def remake_report_file(self, report_id: int):
        # TODO: Сгенерировать файл из ранее сохраненного в БД отчета
        pass
