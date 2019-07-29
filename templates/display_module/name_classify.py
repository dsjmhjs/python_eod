# __auther__ = luolinhua

long_list = ['ZZ500', 'CSI300', 'ZZ800', 'WindA', 'YSPool2']
hedge_list = ['IF', 'IC']
hedge_long_list = list(set(long_list).union(set(hedge_list)))
ban_start_title = hedge_long_list


if __name__ == '__main__':
    print hedge_long_list
