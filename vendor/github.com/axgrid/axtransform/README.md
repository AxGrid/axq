AxTransform
===========

Трансформирует объект через миддлвари

```go

type testStructA struct {
	A int
}

type testStructB struct {
	B int
}

func TestAxTransform_Transform(t *testing.T) {
	transform := NewAxTransform[*testStructA, *testStructB]().
		WithMiddlewares(
			func(c *TransformContext[*testStructA, *testStructB]) {
				c.Next()
				println("From:", c.From.A, "To:", c.To.B)
			},
			func(c *TransformContext[*testStructA, *testStructB]) {
				c.To = &testStructB{}
				c.Next()
			},
			func(c *TransformContext[*testStructA, *testStructB]) {
				c.To.B = c.From.A * 2
				c.Next()
			},
		).
		Build()
	a := &testStructA{A: 100}
	b, err := transform.Transform(a)
    println(b.B)
}


```

RESULT:
```
From: 100 To: 200
200
```