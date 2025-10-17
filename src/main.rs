#[macro_use]
extern crate rocket;

mod account;

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![account::connect_account])
}
