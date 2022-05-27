ns_help
=======

fast parsing of a complete NetString with sub NetStrings

very simple API:

```python
>>> import ns_help
>>> cols = ns_help.get_substrings()
>>> for type, data in cols:
>>>    if type != 3:
>>>        raise "Unknown Type"
>>>    print "Got column ", data
...
```

