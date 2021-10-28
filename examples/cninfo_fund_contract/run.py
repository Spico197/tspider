import sys
sys.path.insert(0, "/data/tzhu/BidSpider")

from cninfo_fund_contract import CnInfoFundContractSpider


if __name__ == "__main__":
    config_filepath = "examples/cninfo_fund_contract/config.yaml"
    spider = CnInfoFundContractSpider(config_filepath)
    spider.start()
