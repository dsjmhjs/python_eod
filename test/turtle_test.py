# -*- coding: utf-8 -*-
# import turtle
# import random
# from turtle import *
# from time import sleep

# t = turtle.Turtle()
# w = turtle.Screen()
#
#
# def tree(branchLen, t):
#     if branchLen > 3:
#         if 8 <= branchLen <= 12:
#             if random.randint(0, 2) == 0:
#                 t.color('snow')
#             else:
#                 t.color('lightcoral')
#             t.pensize(branchLen / 3)
#         elif branchLen < 8:
#             if random.randint(0, 1) == 0:
#                 t.color('snow')
#             else:
#                 t.color('lightcoral')
#             t.pensize(branchLen / 2)
#         else:
#             t.color('sienna')
#             t.pensize(branchLen / 10)
#
#         t.forward(branchLen)
#         a = 1.5 * random.random()
#         t.right(20*a)
#         b = 1.5 * random.random()
#         tree(branchLen-10*b, t)
#         t.left(40*a)
#         tree(branchLen-10*b, t)
#         t.right(20*a)
#         t.up()
#         t.backward(branchLen)
#         t.down()
#
#
# def petal(m, t):  # 树下花瓣
#     for i in range(m):
#         a = 200 - 400 * random.random()
#         b = 10 - 20 * random.random()
#         t.up()
#         t.forward(b)
#         t.left(90)
#         t.forward(a)
#         t.down()
#         t.color("lightcoral")
#         t.circle(1)
#         t.up()
#         t.backward(a)
#         t.right(90)
#         t.backward(b)
#
#
# def main():
#     t = turtle.Turtle()
#     myWin = turtle.Screen()
#     getscreen().tracer(5, 0)
#     turtle.screensize(bg='wheat')
#     t.left(90)
#     t.up()
#     t.backward(150)
#     t.down()
#     t.color('sienna')
#     tree(60, t)
#     petal(100, t)
#
#     myWin.exitonclick()

# main()


import turtle


def draw_triangle(points, color, t):
    t.fillcolor(color)
    t.up()
    t.goto(points[0][0], points[0][1])
    t.down()
    t.begin_fill()
    t.goto(points[1][0], points[1][1])
    t.goto(points[2][0], points[2][1])
    t.goto(points[0][0], points[0][1])
    t.end_fill()


def get_mid(point1, point2):
    return (point1[0] + point2[0]) / 2, (point1[1] + point2[1]) / 2


def sierpinski(points, degree, t):
    color_map = ['blue', 'red', 'green', 'yellow', 'violet', 'orange', 'white',]

    draw_triangle(points, color_map[degree], t)

    if degree > 0:
        sierpinski([points[0], get_mid(points[0], points[1]), get_mid(points[0], points[2])], degree - 1, t)

        sierpinski([points[1], get_mid(points[0], points[1]), get_mid(points[1], points[2])], degree - 1, t)

        sierpinski([points[2], get_mid(points[0], points[2]), get_mid(points[1], points[2])], degree - 1, t)


if __name__ == "__main__":
    t = turtle.Turtle()
    t.speed(5)
    win = turtle.Screen()

    points = [[-100, -50], [0, 100], [100, -50]]
    sierpinski(points, 3, t)

    win.exitonclick()