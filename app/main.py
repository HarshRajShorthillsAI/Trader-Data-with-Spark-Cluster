from trader_pipeline import Trader_Pipeline

class Main:
    @staticmethod
    def main():
        Trader_Pipeline("/asset").task2()

if __name__=='__main__':
    Main.main()