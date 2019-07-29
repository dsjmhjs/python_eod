# -*- coding: utf-8 -*-
from argparse import ArgumentParser


def parse_arguments():
    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--date",
        dest="date",
        help='input filter date',
        default=''
    )

    parser.add_argument(
        "-s",
        "--screen_name",
        dest="screen_name",
        help='input screen name',
        default=''
    )

    parser.add_argument(
        "-c",
        "--command",
        dest="command",
        help='input your command',
        default='ls'
    )

    options = parser.parse_args()
    return options