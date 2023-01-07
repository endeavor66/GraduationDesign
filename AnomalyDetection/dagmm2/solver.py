import datetime
import time
import IPython
import matplotlib.pyplot as plt
from tqdm import tqdm
from AnomalyDetection.dagmm2.data_loader import *
from AnomalyDetection.dagmm2.model import *
from AnomalyDetection.dagmm2.Config import Config
from utils import *


class Solver(object):
    DEFAULTS = {}
    def __init__(self, data_loader, config):
        # Data loader
        self.__dict__.update(Solver.DEFAULTS, **config)
        self.data_loader = data_loader

        self.build_model()

        # if self.use_tensorboard:
        #     self.build_tensorboard()

        # Start with trained model
        if self.pretrained_model:
            self.load_pretrained_model()

    def build_model(self):
        # Define model
        self.dagmm = DaGMM(self.gmm_k)

        # Optimizers
        self.optimizer = torch.optim.Adam(self.dagmm.parameters(), lr=self.lr)

        # Print networks
        self.print_network(self.dagmm, 'DaGMM')

        if torch.cuda.is_available():
            self.dagmm.cuda()

    def print_network(self, model, name):
        num_params = 0
        for p in model.parameters():
            num_params += p.numel()
        print(name)
        print(model)
        print("The number of parameters: {}".format(num_params))

    def load_pretrained_model(self):
        self.dagmm.load_state_dict(torch.load(os.path.join(
            self.model_save_path, '{}_dagmm.pth'.format(self.pretrained_model))))

        print("phi", self.dagmm.phi,"mu",self.dagmm.mu, "cov",self.dagmm.cov)

        print('loaded trained models (step: {})..!'.format(self.pretrained_model))

    def build_tensorboard(self):
        from AnomalyDetection.dagmm2.logger import Logger
        self.logger = Logger(self.log_path)

    def reset_grad(self):
        self.dagmm.zero_grad()

    def to_var(self, x, volatile=False):
        if torch.cuda.is_available():
            x = x.cuda()
        return Variable(x, volatile=volatile)

    def train(self, repo, role):
        iters_per_epoch = len(self.data_loader)

        # Start with trained model if exists
        if self.pretrained_model:
            start = int(self.pretrained_model.split('_')[0])
        else:
            start = 0

        # Start training
        iter_ctr = 0
        start_time = time.time()

        self.ap_global_train = np.array([0, 0, 0])
        for e in range(start, self.num_epochs):
            for i, input_data in enumerate(tqdm(self.data_loader)):
                iter_ctr += 1
                start = time.time()
                input_data = self.to_var(input_data)
                total_loss, sample_energy, recon_error, cov_diag = self.dagmm_step(input_data)
                # Logging
                loss = {}
                loss['total_loss'] = total_loss.data.item()
                loss['sample_energy'] = sample_energy.item()
                loss['recon_error'] = recon_error.item()
                loss['cov_diag'] = cov_diag.item()


                # 输出训练过程
                if (i+1) % self.log_step == 0:
                    elapsed = time.time() - start_time
                    total_time = ((self.num_epochs*iters_per_epoch)-(e*iters_per_epoch+i)) * elapsed/(e*iters_per_epoch+i+1)
                    epoch_time = (iters_per_epoch-i)* elapsed/(e*iters_per_epoch+i+1)
                    
                    epoch_time = str(datetime.timedelta(seconds=epoch_time))
                    total_time = str(datetime.timedelta(seconds=total_time))
                    elapsed = str(datetime.timedelta(seconds=elapsed))

                    lr_tmp = []
                    for param_group in self.optimizer.param_groups:
                        lr_tmp.append(param_group['lr'])
                    tmplr = np.squeeze(np.array(lr_tmp))

                    log = "Elapsed {}/{} -- {} , Epoch [{}/{}], Iter [{}/{}], lr {}".format(
                        elapsed,epoch_time,total_time, e+1, self.num_epochs, i+1, iters_per_epoch, tmplr)

                    for tag, value in loss.items():
                        log += ", {}: {:.4f}".format(tag, value)

                    IPython.display.clear_output()
                    print(log)

                    plt_ctr = 1
                    if not hasattr(self,"loss_logs"):
                        self.loss_logs = {}
                        for loss_key in loss:
                            self.loss_logs[loss_key] = [loss[loss_key]]
                            plt.subplot(2,2,plt_ctr)
                            plt.plot(np.array(self.loss_logs[loss_key]), label=loss_key)
                            plt.legend()
                            plt_ctr += 1
                    else:
                        for loss_key in loss:
                            self.loss_logs[loss_key].append(loss[loss_key])
                            plt.subplot(2,2,plt_ctr)
                            plt.plot(np.array(self.loss_logs[loss_key]), label=loss_key)
                            plt.legend()
                            plt_ctr += 1

                    plt.show()

                    print("phi", self.dagmm.phi,"mu",self.dagmm.mu, "cov",self.dagmm.cov)
                # # Save model checkpoints
                # if (i+1) % self.model_save_step == 0:
                #     torch.save(self.dagmm.state_dict(),
                #         os.path.join(self.model_save_path, '{}_{}_dagmm.pth'.format(e+1, i+1)))
        #保存最终模型
        save_file_path = f"{Config.MODEL_DIR}/{repo}_{role}_dagmm.pth"
        torch.save(self.dagmm.state_dict(), save_file_path)

    def dagmm_step(self, input_data):
        self.dagmm.train()

        # 降维后的特征，还原后的特征，拼接后的三维特征，归属概率   调用dagmm的forward完成一次正向传播
        enc, dec, z, gamma = self.dagmm(input_data)

        # 估计层的损失（论文内定义），样本损失，重构误差，协方差
        total_loss, sample_energy, recon_error, cov_diag = self.dagmm.loss_function(input_data, dec, z, gamma, self.lambda_energy, self.lambda_cov_diag)

        # 反向传播
        self.reset_grad()
        total_loss.backward()

        #优化
        torch.nn.utils.clip_grad_norm_(self.dagmm.parameters(), 5)
        self.optimizer.step()

        return total_loss,sample_energy, recon_error, cov_diag

    def test(self, repo, role):
        print("======================根据模型计算异常得分======================")
        # self.dagmm.eval()

        N = 0
        mu_sum = 0
        cov_sum = 0
        gamma_sum = 0

        input_path = f"{Config.FEATURE_DIR}/{repo}_{role}_feature.csv"
        source_data = pd.read_csv(input_path)

        # 加载已训练好的模型
        model = DaGMM(self.gmm_k)
        model_path = f"{Config.MODEL_DIR}/{repo}_{role}_dagmm.pth"
        model.load_state_dict(torch.load(model_path))
        # 使用模型时调用eval模式会自动关闭dropout层
        model.eval()

        print("phi", model.phi, "mu", model.mu, "cov", model.cov)

        source_data['score'] = float(0)
        for i in range(0, len(source_data)):
            instance = source_data.iloc[i]
            data = instance[Config.train_col].to_numpy()
            data = data.astype(float)
            data = torch.tensor(data).float()
            data = data.unsqueeze(0).float()
            enc, dec, z, gamma = model(torch.tensor(data).float())
            gamma = gamma.squeeze(0)
            score_list = gamma.detach().numpy()
            instance['score'] = max(score_list)
            # print(str(instance['id'] + ': ' + str(gamma) + ' max: ' + str(instance['score'])))
            source_data.iloc[i] = instance

        max_min_scaler = lambda x: (x - np.min(x)) / (np.max(x) - np.min(x))
        source_data[['score']] = source_data[['score']].apply(max_min_scaler)

        output_path = f"{Config.DAGMM_OUTPUT_DIR}/{repo}_{role}_dagmm.csv"
        source_data.to_csv(output_path, index=False, header=True)


