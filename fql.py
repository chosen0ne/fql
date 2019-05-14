#!/usr/bin/env python
# coding: utf8
#
#
# @file:    fql
# @author:  chosen0ne(louzhenlin86@126.com)
# @date:    2015/01/22 11:53:19

import cmd
import sys
from optparse import OptionParser
from grammar_parser import yacc

_fql_version = '0.1.0'


class FqlCmd(cmd.Cmd):
    def __init__(self):
        cmd.Cmd.__init__(self)
        self.prompt = 'fql> '

    def do_select(self, arg):
        '''
        use fql to query file infos
        '''
        yacc.parse('select ' + arg)

    def do_exit(self, arg):
        '''
        exit fql command line interpreter
        '''
        sys.exit()

    def do_EOF(self, line):
        return True

    def do_q(self, arg):
        '''
        exit fql command line interpreter
        '''
        sys.exit()


def opt_parse():
    usage = 'USAGE: %prog [sql statement]'
    parser = OptionParser(usage=usage)
    parser.add_option('-v', '--version', dest='show_version', default=False,
                      help='show version info', action='store_true')
    parser.add_option('-d', '--max-depth', dest='depth', default=3,
                      type='int', help='max depth to travel')

    return parser.parse_args()


def show_version():
    print 'fql "%s"' % _fql_version


if __name__ == '__main__':
    opt, args = opt_parse()
    if opt.show_version:
        show_version()
        sys.exit()

    if args:
        yacc.parse(' '.join(args))
        sys.exit()

    c = FqlCmd()
    c.cmdloop()
