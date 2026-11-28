extern "C" {
    fn add_c(a: i32, b: i32) -> i32;
}

fn main() {
    let sum = unsafe { add_c(40, 2) };
    println!("add_c(40, 2) = {sum}");
    assert_eq!(sum, 42);
}
