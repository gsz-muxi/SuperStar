# -*- coding: utf-8 -*-
import argparse
import configparser
import random
import time
import sys
import os
import traceback
import threading
import queue
import pymysql
from multiprocessing import Pool, cpu_count
from urllib3 import disable_warnings, exceptions

from api.logger import logger
from api.base import Chaoxing, Account
from api.exceptions import LoginError, InputFormatError, MaxRollBackExceeded
from api.answer import Tiku
from api.notification import Notification

# 关闭警告
disable_warnings(exceptions.InsecureRequestWarning)


class DatabaseManager:
    """数据库管理器"""
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.load_config()
    
    def load_config(self):
        """加载配置文件"""
        try:
            self.config.read('config.ini', encoding='utf-8')
        except Exception as e:
            logger.error(f"配置加载失败: {str(e)}")
            raise
    
    def get_db_config(self):
        """获取数据库配置"""
        return {
            'host': self.config.get('database', 'host', fallback='localhost'),
            'port': self.config.getint('database', 'port', fallback=3306),
            'user': self.config.get('database', 'user', fallback='wk'),
            'password': self.config.get('database', 'password', fallback='1nnT4THKNez3p8p7'),
            'database': self.config.get('database', 'db', fallback='wk'),
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }


class OrderManager:
    """订单管理器 - 用于从数据库获取任务"""
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.order_queue = queue.Queue()
        self.fetch_interval = 30  # 获取订单间隔(秒)
        self.max_workers = cpu_count() * 2  # 最大工作进程数
        self.processed_orders = set()  # 已处理订单ID集合
        self.lock = threading.Lock()  # 线程锁
    
    def start_fetcher(self):
        """启动订单获取线程"""
        def fetcher():
            while True:
                try:
                    orders = self._fetch_orders()
                    if orders:
                        for order in orders:
                            with self.lock:
                                if order['oid'] not in self.processed_orders:
                                    self.order_queue.put(order)
                                    self.processed_orders.add(order['oid'])
                                    logger.info(f"新订单入队列: #{order['oid']}")
                    
                    # 动态调整获取间隔
                    queue_size = self.order_queue.qsize()
                    if queue_size > 20:
                        sleep_time = self.fetch_interval * 2
                    elif queue_size > 10:
                        sleep_time = self.fetch_interval * 1.5
                    else:
                        sleep_time = self.fetch_interval
                    
                    time.sleep(sleep_time)
                except Exception as e:
                    logger.error(f"订单获取异常: {str(e)}")
                    time.sleep(60)
        
        threading.Thread(target=fetcher, daemon=True).start()
    
    def _fetch_orders(self):
        """从数据库获取订单"""
        try:
            database_config = self.db_manager.get_db_config()
            with pymysql.connect(**database_config) as conn:
                with conn.cursor() as cursor:
                    # 使用事务和行锁
                    conn.begin()
                    
                    # 查询待处理订单(按oid排序保证一致性)
                    cursor.execute("""
                        SELECT * FROM qingka_wangke_order 
                        WHERE dockstatus=0 AND status IN ('待处理', '补刷中')
                        ORDER BY oid ASC
                        LIMIT 10
                        FOR UPDATE
                    """)
                    orders = cursor.fetchall()
                    
                    if orders:
                        # 标记订单为处理中
                        oids = [str(o['oid']) for o in orders]
                        cursor.execute(f"""
                            UPDATE qingka_wangke_order 
                            SET status='处理中', remarks='已加入处理队列'
                            WHERE oid IN ({','.join(oids)})
                        """)
                        conn.commit()
                    return orders
        except Exception as e:
            logger.error(f"订单获取失败: {str(e)}")
            return []


class RollBackManager:
    """课程回滚管理器，避免无限回滚"""
    def __init__(self):
        self.rollback_times = 0
        self.rollback_id = ""
    
    def add_times(self, id: str):
        """增加回滚次数"""
        if id == self.rollback_id and self.rollback_times == 3:
            raise MaxRollBackExceeded("回滚次数已达3次, 请手动检查学习通任务点完成情况")
        else:
            self.rollback_times += 1
    
    def new_job(self, id: str):
        """设置新任务，重置回滚次数"""
        if id != self.rollback_id:
            self.rollback_id = id
            self.rollback_times = 0


def init_chaoxing_from_order(order, tiku_config):
    """从订单初始化超星实例"""
    username = order['user']
    password = order['pass']
    course_list = [order['kcid']]  # 从订单中获取课程ID
    
    account = Account(username, password)
    
    # 设置题库
    tiku = Tiku()
    tiku.config_set(tiku_config)  # 载入配置
    tiku = tiku.get_tiku_from_config()  # 载入题库
    tiku.init_tiku()  # 初始化题库
    
    # 获取查询延迟设置
    query_delay = tiku_config.get("delay", 0)
    
    # 实例化超星API
    chaoxing = Chaoxing(account=account, tiku=tiku, query_delay=query_delay)
    
    return chaoxing, course_list


def handle_not_open_chapter(notopen_action, point, tiku, RB, auto_skip_notopen=False):
    """处理未开放章节"""
    if notopen_action == "retry":
        # 默认处理方式：重试
        # 针对题库启用情况
        if not tiku or tiku.DISABLE or not tiku.SUBMIT:
            # 未启用题库或未开启题库提交, 章节检测未完成会导致无法开始下一章, 直接退出
            logger.error(
                "章节未开启, 可能由于上一章节的章节检测未完成, 也可能由于该章节因为时效已关闭，"
                "请手动检查完成并提交再重试。或者在配置中配置(自动跳过关闭章节/开启题库并启用提交)"
            )
            return -1  # 退出标记
        RB.add_times(point["id"])
        return 0  # 重试上一章节
    
    elif notopen_action == "ask":
        # 询问模式 - 判断是否需要询问
        if not auto_skip_notopen:
            user_choice = input(f"章节 {point['title']} 未开放，是否继续检查后续章节？(y/n): ")
            if user_choice.lower() != 'y':
                # 用户选择停止
                logger.info("根据用户选择停止检查后续章节")
                return -1  # 退出标记
            # 用户选择继续，设置自动跳过标志
            logger.info("用户选择继续检查后续章节，将自动跳过连续的未开放章节")
            return 1, True  # 继续下一章节, 设置自动跳过
        else:
            logger.info(f"章节 {point['title']} 未开放，自动跳过")
            return 1, auto_skip_notopen  # 继续下一章节, 保持自动跳过状态
    
    else:  # notopen_action == "continue"
        # 继续模式，直接跳过当前章节
        logger.info(f"章节 {point['title']} 未开放，根据配置跳过此章节")
        return 1  # 继续下一章节


def process_job(chaoxing, course, job, job_info, speed):
    """处理单个任务点"""
    # 视频任务
    if job["type"] == "video":
        logger.trace(f"识别到视频任务, 任务章节: {course['title']} 任务ID: {job['jobid']}")
        # 超星的接口没有返回当前任务是否为Audio音频任务
        video_result = chaoxing.study_video(
            course, job, job_info, _speed=speed, _type="Video"
        )
        if chaoxing.StudyResult.is_failure(video_result):
            logger.warning("当前任务非视频任务, 正在尝试音频任务解码")
            video_result = chaoxing.study_video(
                course, job, job_info, _speed=speed, _type="Audio")
        if chaoxing.StudyResult.is_failure(video_result):
            logger.warning(
                f"出现异常任务 -> 任务章节: {course['title']} 任务ID: {job['jobid']}, 已跳过"
            )
    # 文档任务
    elif job["type"] == "document":
        logger.trace(f"识别到文档任务, 任务章节: {course['title']} 任务ID: {job['jobid']}")
        chaoxing.study_document(course, job)
    # 测验任务
    elif job["type"] == "workid":
        logger.trace(f"识别到章节检测任务, 任务章节: {course['title']}")
        chaoxing.study_work(course, job, job_info)
    # 阅读任务
    elif job["type"] == "read":
        logger.trace(f"识别到阅读任务, 任务章节: {course['title']}")
        chaoxing.strdy_read(course, job, job_info)


def process_chapter(chaoxing, course, point, RB, notopen_action, speed, auto_skip_notopen=False):
    """处理单个章节"""
    logger.info(f'当前章节: {point["title"]}')
    
    if point["has_finished"]:
        logger.info(f'章节：{point["title"]} 已完成所有任务点')
        return 1, auto_skip_notopen  # 继续下一章节
    
    # 获取当前章节的所有任务点
    jobs = []
    job_info = None
    jobs, job_info = chaoxing.get_job_list(
        course["clazzId"], course["courseId"], course["cpi"], point["id"]
    )
    
    # 发现未开放章节, 根据配置处理
    try:
        if job_info.get("notOpen", False):
            result = handle_not_open_chapter(
                notopen_action, point, chaoxing.tiku, RB, auto_skip_notopen
            )
            
            if isinstance(result, tuple):
                return result  # 返回继续标志和更新后的auto_skip_notopen
            else:
                return result, auto_skip_notopen
        
        # 遇到开放的章节，重置自动跳过状态
        auto_skip_notopen = False
        RB.new_job(point["id"])
    
    except MaxRollBackExceeded:
        logger.error("回滚次数已达3次, 请手动检查学习通任务点完成情况")
        # 跳过该课程
        return -1, auto_skip_notopen  # 退出标记
    
    chaoxing.rollback_times = RB.rollback_times
    
    # 可能存在章节无任何内容的情况
    if not jobs:
        if RB.rollback_times > 0:
            logger.trace(f"回滚中 尝试空页面任务, 任务章节: {course['title']}")
            chaoxing.study_emptypage(course, point)
        return 1, auto_skip_notopen  # 继续下一章节
    
    # 遍历所有任务点
    for job in jobs:
        process_job(chaoxing, course, job, job_info, speed)
    
    return 1, auto_skip_notopen  # 继续下一章节


def process_course(chaoxing, course, notopen_action, speed):
    """处理单个课程"""
    logger.info(f"开始学习课程: {course['title']}")
    # 获取当前课程的所有章节
    point_list = chaoxing.get_course_point(
        course["courseId"], course["clazzId"], course["cpi"]
    )
    
    # 为了支持课程任务回滚, 采用下标方式遍历任务点
    __point_index = 0
    # 记录用户是否选择继续跳过连续的未开放任务点
    auto_skip_notopen = False
    # 初始化回滚管理器
    RB = RollBackManager()
    
    while __point_index < len(point_list["points"]):
        point = point_list["points"][__point_index]
        
        result, auto_skip_notopen = process_chapter(
            chaoxing, course, point, RB, notopen_action, speed, auto_skip_notopen
        )
        
        if result == -1:  # 退出当前课程
            break
        elif result == 0:  # 重试前一章节
            __point_index -= 1  # 默认第一个任务总是开放的
        else:  # 继续下一章节
            __point_index += 1


def filter_courses(all_course, course_list):
    """过滤要学习的课程"""
    # 筛选需要学习的课程
    course_task = []
    for course in all_course:
        if str(course["courseId"]) in course_list:
            course_task.append(course)
    
    # 如果没有匹配的课程，则学习所有课程
    if not course_task:
        course_task = all_course
    
    return course_task


def update_order_status(db_manager, oid, status, remarks, process=None):
    """更新订单状态"""
    try:
        with pymysql.connect(**db_manager.get_db_config()) as conn:
            with conn.cursor() as cursor:
                if process is not None:
                    sql = """UPDATE qingka_wangke_order
                            SET status=%s, remarks=%s, process=%s
                            WHERE oid=%s"""
                    cursor.execute(sql, (status, remarks, process, oid))
                else:
                    sql = """UPDATE qingka_wangke_order 
                            SET status=%s, remarks=%s
                            WHERE oid=%s"""
                    cursor.execute(sql, (status, remarks, oid))
            conn.commit()
    except Exception as e:
        logger.error(f"状态更新失败: {str(e)}")


def process_order(order, common_config, tiku_config, notification_config, db_manager):
    """处理单个订单"""
    oid = order['oid']
    logger.info(f"开始处理订单 #{oid}")
    max_retries = 3
    
    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            logger.info(f"重试处理订单 #{oid} (尝试 {attempt}/{max_retries})")
            update_order_status(db_manager, oid, '补刷中', f'第 {attempt} 次尝试处理')
            time.sleep(10)  # 重试前等待一段时间
        
        try:
            # 从订单获取配置
            speed = min(2.0, max(1.0, common_config.get("speed", 1.0)))
            notopen_action = common_config.get("notopen_action", "retry")
            
            # 初始化超星实例
            update_order_status(db_manager, oid, '处理中', '初始化超星实例', '0%')
            chaoxing, course_list = init_chaoxing_from_order(order, tiku_config)
            
            # 设置外部通知
            notification = Notification()
            notification.config_set(notification_config)
            notification = notification.get_notification_from_config()
            notification.init_notification()
            
            # 检查当前登录状态
            update_order_status(db_manager, oid, '处理中', '正在登录', '10%')
            _login_state = chaoxing.login()
            if not _login_state["status"]:
                raise LoginError(_login_state["msg"])
            
            # 获取所有的课程列表
            all_course = chaoxing.get_course_list()
            
            # 过滤要学习的课程
            update_order_status(db_manager, oid, '处理中', '筛选课程', '20%')
            course_task = filter_courses(all_course, course_list)
            
            if not course_task:
                update_order_status(db_manager, oid, '已完成', '没有找到指定的课程', '100%')
                logger.info(f"订单 #{oid} 没有找到指定的课程")
                return True
            
            # 开始学习
            logger.info(f"订单 #{oid} 课程列表过滤完毕, 当前课程任务数量: {len(course_task)}")
            
            total_courses = len(course_task)
            for idx, course in enumerate(course_task):
                process_percent = 20 + int((idx / total_courses) * 80)
                update_order_status(db_manager, oid, '处理中', 
                                   f'正在学习课程: {course["title"]}', 
                                   f'{process_percent}%')
                
                process_course(chaoxing, course, notopen_action, speed)
            
            update_order_status(db_manager, oid, '已完成', '所有课程学习任务已完成', '100%')
            logger.info(f"订单 #{oid} 所有课程学习任务已完成")
            
            try:
                notification.send(f"chaoxing : 订单 #{oid} 所有课程学习任务已完成")
            except Exception:
                pass  # 如果通知发送失败，忽略异常
            
            return True  # 成功完成
        
        except Exception as e:
            error_msg = str(e)
            logger.error(f"订单 #{oid} 尝试 {attempt} 失败: {error_msg}")
            
            if attempt < max_retries:
                continue  # 继续下一次重试
            else:
                # 所有重试都失败
                update_order_status(db_manager, oid, '失败', 
                                   f'错误: {error_msg}，已重试 {max_retries} 次')
                return False


def load_config_from_file(config_path):
    """从配置文件加载设置"""
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf8")
    
    common_config = {}
    tiku_config = {}
    notification_config = {}
    
    # 检查并读取common节
    if config.has_section("common"):
        common_config = dict(config.items("common"))
        # 处理speed，将字符串转换为浮点数
        if "speed" in common_config:
            common_config["speed"] = float(common_config["speed"])
        # 处理notopen_action，设置默认值为retry
        if "notopen_action" not in common_config:
            common_config["notopen_action"] = "retry"
    
    # 检查并读取tiku节
    if config.has_section("tiku"):
        tiku_config = dict(config.items("tiku"))
        # 处理数值类型转换
        for key in ["delay", "cover_rate"]:
            if key in tiku_config:
                tiku_config[key] = float(tiku_config[key])
    
    # 检查并读取notification节
    if config.has_section("notification"):
        notification_config = dict(config.items("notification"))
    
    return common_config, tiku_config, notification_config


def main():
    """主程序入口"""
    try:
        # 创建默认配置文件（如果不存在）
        if not os.path.exists('config.ini'):
            config = configparser.ConfigParser()
            config.add_section('database')
            config.set('database', 'host', 'localhost')
            config.set('database', 'port', '3306')
            config.set('database', 'user', 'iwk_asia')
            config.set('database', 'password', '1nnT4THKNez3p8p7')
            config.set('database', 'db', 'iwk_asia')
            
            config.add_section('common')
            config.set('common', 'speed', '1.0')
            config.set('common', 'notopen_action', 'retry')
            
            with open('config.ini', 'w', encoding='utf-8') as f:
                config.write(f)
            logger.info("已创建默认配置文件 config.ini")
        
        # 加载配置
        common_config, tiku_config, notification_config = load_config_from_file('config.ini')
        
        # 初始化数据库管理器
        db_manager = DatabaseManager()
        
        # 初始化订单管理器
        order_manager = OrderManager(db_manager)
        
        # 启动订单获取线程
        order_manager.start_fetcher()
        
        logger.info("超星多账号自动化处理系统已启动")
        logger.info(f"最大工作进程数: {order_manager.max_workers}")
        
        # 启动工作进程池
        with Pool(processes=order_manager.max_workers) as pool:
            while True:
                try:
                    if not order_manager.order_queue.empty():
                        order = order_manager.order_queue.get()
                        
                        # 使用进程池异步处理订单
                        pool.apply_async(
                            process_order,
                            args=(order, common_config, tiku_config, notification_config, db_manager),
                            error_callback=lambda e: logger.error(f"工作进程异常: {str(e)}")
                        )
                    else:
                        time.sleep(1)
                except KeyboardInterrupt:
                    logger.info("程序被用户中断，正在退出...")
                    break
                except Exception as e:
                    logger.error(f"主程序异常: {str(e)}")
                    time.sleep(5)
    
    except SystemExit as e:
        if e.code != 0:
            logger.error(f"错误: 程序异常退出, 返回码: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        logger.info("程序被用户手动中断")
        sys.exit(0)
    except BaseException as e:
        logger.error(f"错误: {type(e).__name__}: {e}")
        logger.error(traceback.format_exc())
        raise e


if __name__ == "__main__":
    main()
