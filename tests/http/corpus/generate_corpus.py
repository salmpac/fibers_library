#!/usr/bin/env python3
"""Generates upstream_cases.inc: HTTP/1.1 request test cases taken from the
test suites of llhttp and picohttpparser (both MIT; see THIRD_PARTY_NOTICES
in this directory), replayed through our server by http_corpus_test.cpp.

    generate_corpus.py <llhttp source tree> <picohttpparser test.c> > upstream_cases.inc

llhttp (test/request/*.md): every test of type "request" (the strict
default mode; the lenient-* types are skipped, as is skipBody, which changes
the callback contract). Input bytes are normalized exactly as llhttp's
test/md-test.ts does (then UTF-8, as its fixture passes them). Expectation from the expected callback log: the number
of "message complete" lines, and whether it ends in an error, with an
incomplete message, or in a CONNECT / Upgrade pause.

picohttpparser (test.c): test_request's PARSE() cases (0: complete,
-1: error, -2: incomplete) and the chunked decoder cases, wrapped into a
chunked POST (decoded body checked where the message is complete).
"""

import json
import re
import sys
from pathlib import Path

OK, ERROR, INCOMPLETE, UPGRADE = 0, 1, 2, 3


def c_literal(data: bytes) -> str:
    out = []
    for b in data:
        c = chr(b)
        if c == '"' or c == '\\':
            out.append('\\' + c)
        elif 0x20 <= b < 0x7F and c != '?':
            out.append(c)
        else:
            out.append('\\%03o' % b)
    return '"' + ''.join(out) + '"'


def normalize_llhttp_input(text: str) -> bytes:
    # test/md-test.ts, in the same order.
    text = re.sub(r'\n\Z', '', text)  # JS /\n$/: the very end only
    text = re.sub(r'\\(\r\n|\r|\n)', '', text)
    text = re.sub(r'\r\n|\r|\n', '\r\n', text)
    text = text.replace('\\r', '\r').replace('\\n', '\n').replace('\\t', '\t').replace('\\f', '\f')
    text = re.sub(r'\\x([0-9a-fA-F]+)', lambda m: chr(int(m.group(1), 16)), text)
    text = re.sub(r'\\([0-7]{1,3})', lambda m: chr(int(m.group(1), 8)), text)
    if '${' in text:
        raise ValueError('inline JavaScript not supported')
    # The harness hands the JS string over as UTF-8 (llparse-test-fixture).
    return text.encode('utf-8')


def llhttp_cases(root: Path):
    cases = []
    for md in sorted((root / 'test' / 'request').glob('*.md')):
        lines = md.read_text(encoding='utf-8').split('\n')
        heading = ''
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.startswith('#'):
                heading = line.lstrip('#').strip()
            m = re.match(r'<!--\s*meta=(.*?)\s*-->', line)
            if not m:
                i += 1
                continue
            meta = json.loads(m.group(1))
            start = i + 1
            # ```http block
            while not lines[start].startswith('```http'):
                start += 1
            end = start + 1
            while lines[end] != '```':
                end += 1
            body = '\n'.join(lines[start + 1:end]) + '\n'
            # ```log block
            lstart = end + 1
            while not lines[lstart].startswith('```log'):
                lstart += 1
            lend = lstart + 1
            while lines[lend] != '```':
                lend += 1
            log = lines[lstart + 1:lend]
            i = lend + 1
            if meta.get('type') != 'request' or meta.get('skipBody'):
                continue
            data = normalize_llhttp_input(body)
            completes = sum(1 for l in log if l.endswith('message complete'))
            begins = sum(1 for l in log if l.endswith('message begin'))
            if any(' error code=22 ' in l for l in log):
                expect = UPGRADE  # HPE_PAUSED_UPGRADE: the rest is another protocol
            elif any(' error code=' in l for l in log):
                expect = ERROR
            elif begins > completes:
                expect = INCOMPLETE
            else:
                expect = OK
            cases.append(('llhttp', f'{md.name}:{start + 1} {heading}', data, expect, completes, None))
    return cases


def c_string(s: str) -> bytes:
    return s.encode('latin-1').decode('unicode_escape').encode('latin-1')


def pico_cases(test_c: Path):
    text = test_c.read_text(encoding='latin-1')
    cases = []
    req = text[text.index('static void test_request(void)'):text.index('static void test_response(void)')]
    for m in re.finditer(r'PARSE\("((?:[^"\\]|\\.)*)",\s*([^,]+),\s*(-?\d+),\s*"((?:[^"\\]|\\.)*)"\)', req):
        data = c_string(m.group(1))
        last_len = m.group(2).strip()
        exp = int(m.group(3))
        name = m.group(4)
        if last_len != '0':
            name += ' (slowloris variant)'
        expect = {0: OK, -1: ERROR, -2: INCOMPLETE}[exp]
        cases.append(('picohttpparser', f'test_request: {name}', data, expect, 1 if expect == OK else 0, None))

    prefix = b'POST /chunked HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n'
    seen = set()
    runner = re.compile(r'chunked_test_runners\[i\]\(__LINE__,\s*(\d),\s*"((?:[^"\\]|\\.)*)",\s*"((?:[^"\\]|\\.)*)",\s*(-?\d+)\)')
    for m in runner.finditer(text):
        consume, enc, dec, exp = int(m.group(1)), c_string(m.group(2)), c_string(m.group(3)), int(m.group(4))
        if enc in seen:
            continue
        seen.add(enc)
        # As an HTTP message the chunked body is complete only with the
        # final CRLF (pico: consume_trailer = 1 and 0 returned).
        complete = consume == 1 and exp == 0
        cases.append(('picohttpparser', f'chunked (consume_trailer={consume}): {m.group(2)}', prefix + enc,
                      OK if complete else INCOMPLETE, 1 if complete else 0, dec if complete else None))
    failure = re.compile(r'test_chunked_failure\(__LINE__,\s*"((?:[^"\\]|\\.)*)",\s*(-?\d+)\)')
    for m in failure.finditer(text):
        enc, exp = c_string(m.group(1)), int(m.group(2))
        if enc in seen:
            continue
        seen.add(enc)
        cases.append(('picohttpparser', f'chunked failure: {m.group(1)}', prefix + enc,
                      ERROR if exp == -1 else INCOMPLETE, 0, None))
    return cases


def main():
    llhttp_root, pico_test = Path(sys.argv[1]), Path(sys.argv[2])
    cases = llhttp_cases(llhttp_root) + pico_cases(pico_test)
    print('// Generated by generate_corpus.py from the llhttp and picohttpparser test')
    print('// suites (MIT; see THIRD_PARTY_NOTICES). Do not edit.')
    print('// {source, name, bytes, expect (0 ok / 1 error / 2 incomplete / 3 upgrade), complete messages, body}')
    for source, name, data, expect, messages, body in cases:
        body_lit = 'nullptr' if body is None else c_literal(body.encode('latin-1') if isinstance(body, str) else body)
        print(f'{{"{source}", {c_literal(name.encode("utf-8"))}, std::string_view({c_literal(data)}, {len(data)}), '
              f'{expect}, {messages}, {body_lit}}},')


if __name__ == '__main__':
    main()
