# pq

A tool to transform unstructured text to csv by regular expression.

# Sample

If input is (test.txt)

```html
test=abc&foo=bar1
test=def&foo=bar2
test=efg&foo=bar3
```

Issue this command

```bash
pq -R 'test=(?<test>[a-zA-z]*)&foo=(?<foo>[a-zA-z]*)' -c test,foo test.txt
```

then output will be

```
abc,bar
def,bar
efg,bar
```

In the regular expression, we define two groups 'test', 'foo'. And we translate the group to csv columns.

# Usage

```bash
usage: pq -R <regular-expression> -c <columnlist> [<file> ...]
 -c <arg>   column list. Separated by comma
 -g <arg>   group by list. Separated by comma
 -H         with CSV header
 -P <arg>   level of parallelism
 -R <arg>   the regular expression with groups
```

# Download

Please download the binary from the [release](https://github.com/tenmax/pq/releases) tag.

# Reference

This tool is powered by [poppy](http://tenmax.github.io/poppy/)
