# Java Hadoop Project

This is a java project which using hadoop to analyze **Shakespeare's book**

#### Folder Structure
```sh
  - HW2
    - bin
    - src
        - part1
        - part2
        - part3
        - part4
    - libs
    - tableOutput
    - input
```
Here I only mention the most important part for folder structure.

**part 3** is for hadoop to generate files in **tableOutput** from **input**.

After part 3, **part 4**  will build a terminal prompt for user to input what he/she want to search.

**e.g**

    search "cat"
    search "cat and dog"
    search "cat and dog not duck"

For now, it only support **AND/NOT** and it still lack for lots of tests. 

##Result
`search "cat and dog not duck"`
```sh
1	glossary = 3.233984617446567E-4
************************
cat-gut
cat
dog-rose
dog
dog
dog
************************
2	allswellthatendswell = 2.4551179661610605E-4
************************
cat, and now
cat to me.
cat.
cat still.
cat,--but not a musk-cat,--that has fallen into the
cat,--that has fallen into the
dog-hole, and it no more merits
************************
3	romeoandjuliet = 1.3773112757176984E-4
************************
cat, to scratch a man to death! a braggart, a
cat and dog
dog of the house of Montague moves me.
dog of that house shall move me to stand: I will
dog's name; R is for
dog that hath lain asleep in the sun:
dog, a rat, a mouse, a
dog
************************
4	asyoulikeit = 1.3305347109398836E-4
************************
cat. Mend the instance, shepherd.
cat will after kind,
dog.
dog' my reward? Most true, I have lost my
dog.
dog-apes, and when a man thanks me heartily,
************************
5	rapeoflucrece = 8.160777955889777E-5
************************
cat, he doth but dally,
dog creeps sadly thence;
************************
6	hamlet = 7.676316644257229E-5
************************
cat of the fish that hath fed of that worm.
cat will mew and dog will have his day.
dog, being a
dog will have his day.
************************
7	coriolanus = 5.159621358427399E-5
************************
cat as they did budge
dog to the commonalty.
dog, but for
************************
```
it will rank each file including those words(cat, dog) based on TF*IDF theory.
