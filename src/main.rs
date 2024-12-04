#![feature(random)]
use std::io;

use std::random::random;

fn main() {
    println!("Guess the number");
    println!("Please input your guess.");

    let n = (random::<u32>() % 100) + 1;

    println!("Debug Guessing: {}", n);

    let mut int_guess: u32 = 0;

    while n != int_guess {

    let mut guess = String::new();

    io::stdin()
        .read_line(&mut guess)
        .expect("Failed to read lines");

    int_guess = guess
        .trim()
        .parse()
        .expect("Failed to parse number");

        println!("Sorry you were wrong");
    }
        

    println!("Hey you guess it correct");
    println!("You guessed: {}", int_guess);
}
