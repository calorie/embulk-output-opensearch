# frozen_string_literal: true

require 'json'

data = {
  id: 1,
  name: 'test',
  address: {
    country: 'Japan',
    city: 'Tokyo'
  },
  posts: [
    {
      id: 1,
      text: 'text'
    },
    {
      id: 2,
      text: 'text'
    },
    {
      id: 3,
      text: 'text'
    },
    {
      id: 4,
      text: 'text'
    },
    {
      id: 5,
      text: 'text'
    }
  ]
}
json = JSON.generate(data)

lines = [json] * 5_000_000

File.open('data.json', 'w') do |file|
  file.puts(lines.join("\n"))
end

puts 'fin.'
