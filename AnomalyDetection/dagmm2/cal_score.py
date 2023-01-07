from AnomalyDetection.dagmm2.solver import Solver
from AnomalyDetection.dagmm2.data_loader import get_loader
from torch.backends import cudnn
from AnomalyDetection.dagmm2.Config import Config

def run(config, repo, role):
    # For fast training
    cudnn.benchmark = True

    # 加载特征集，做归一化处理
    data_path = f"{Config.FEATURE_DIR}/{repo}_{role}_feature.csv"
    data_loader = get_loader(data_path, batch_size=config.batch_size)

    # Solver
    solver = Solver(data_loader, vars(config))

    # 训练模型
    solver.train(repo, role)

    # 使用训练好的模型，计算每个样本的异常值
    solver.test(repo, role)

    return solver


def init_train_col(role: str):
    if role == 'committer':
        Config.train_col = ['commit_num', 'total_line_addition', 'total_line_deletion', 'total_file_edit_num',
                            'total_file_edit_type', 'sensitive_line_addition', 'sensitive_line_deletion',
                            'sensitive_file_edit_num', 'merge_rate', 'commit_interval']
    elif role == 'reviewer':
        Config.train_col = ['pr_num', 'review_num',
                            'avg_review_num', 'avg_review_comment_length', 'avg_review_response_time']
    elif role == 'maintainer':
        Config.train_col = ['pr_num', 'merge_rate',
                            'avg_assign_reviewer_num', 'avg_response_time', 'avg_response_interval']


def score(repo, role):
    # 初始化特征列
    init_train_col(role)

    parser = Config.parser

    config = parser.parse_args()

    args = vars(config)

    print('------------ Options -------------')
    for k, v in sorted(args.items()):
        print('%s: %s' % (str(k), str(v)))
    print('-------------- End ----------------')

    run(config, repo, role)


if __name__ == "__main__":
    repos = ['zipkin', 'netbeans', 'opencv', 'dubbo', 'phoenix']
    roles = ["reviewer", "maintainer", "committer"]
    # for repo in repos:
    #     for role in roles:
    #         print(f"{repo} {role} begin")
    #         score(repo, role)
    #         print(f"{repo} {role} process done")
    score("netbeans", "maintainer")