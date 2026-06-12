# CodeSpec for contributor.
### Rule 1: Make sure your code has no side-effect
> * do not use `println()`

### Rule 2: Logging: use Logging trait
>
> ```
> class YourClass extends org.grapheco.pandadb.Logging {
>   logger.debug(s"hello...")
> }
> ```
> Yes! do not use println()!

### Rule 3: Do not keep useless comments!!!
> * wrong comments/validated comments will lead people to a wrong way!
> * please write your comments in English!
> * pay attention to blank lines, remove unnecessary blank lines please.

### Rule 4: Use s"" to format string
> * use: `s"hello, $name"`, do not use: `"hello, "+name`

### Rule 5: Camel case naming style
> * use: `nameOfThisMan`, do not use `this_man`, `UglyMan`, etc
> * do not use Pingyin and unfamiliar abbreviations

### Rule 6: Write test cases
> * write JUnit test cases, instead of write a Java program
> * separate test source code with main source code
> * Use `Assert`, don't judge manually.

### Rule 7: NO hard coding
> * NO magic numbers
>
> * use configurations as possible

### Rule 8: DO not write Scala code in Java style
> * use less `null`, use `Option` instead
> * use more `match` with `case class`
> * use immutable objects, avoiding to use mutable one
> * use functions as possible, for example, `guys.foreach(_.sayHello)`

### Rule 9: Please limit maximum line length

> * Limit all lines to a maximum of 79 characters.

### Rule 10: Do not use var if possible.

> * Use val instead of var, as long as possible.
