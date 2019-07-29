# content of test_sample.py
import smtplib
import pytest


# class TestFixture(object):
#     @pytest.fixture(scope="module", params=["smtp.gmail.com", "mail.python.org"])
#     def smtp(self, request):
#         smtp = smtplib.SMTP(request.param, 587, timeout=5)
#         yield smtp
#         print ("finalizing %s" % smtp)
#         smtp.close()
#
#     def test_smtp(self, smtp):
#         print smtp
#         assert 1 == 2


@pytest.mark.parametrize("test_input,expected", [
    ("3+5", 8),
    ("2+4", 6),
    # ("6*9", 42),
])
def test_eval(test_input, expected):
    assert eval(test_input) == expected
