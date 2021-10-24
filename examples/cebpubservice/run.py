import sys
sys.path.insert(0, "/data/tzhu/BidSpider")

from cebpubservice import CebPubServiceSpider


if __name__ == "__main__":
    config_filepath = "examples/cebpubservice/config.yaml"
    spider = CebPubServiceSpider(config_filepath)
    spider.start()
